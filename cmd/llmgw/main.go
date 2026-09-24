// llmgw 는 설정 파일 기반 LLM 게이트웨이 CLI 다.
//
//	llmgw -config llmgw.toml call -route news-deep "요약할 내용"     # 1회 호출, 결과·시도 기록 출력
//	llmgw -config llmgw.toml call -route news-deep -file article.txt
//	llmgw -config llmgw.toml serve                                   # OpenAI 호환 서버 (model = 라우트 이름)
//	llmgw -config llmgw.toml burst -route fast -n 40 "질문"     # 동시 N 건으로 대기열·폴백 동작 확인
//
// 키는 각 provider 의 api_key_env 환경변수에서 읽는다. -env 파일은 환경변수에 없는 키만 채운다.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/leeyudok/llmgw"
)

func main() {
	home, _ := os.UserHomeDir()
	cfgPath := flag.String("config", "llmgw.toml", "설정 파일(.toml/.yaml)")
	envPath := flag.String("env", filepath.Join(home, ".llmgw.env"), "KEY=VALUE 파일(환경변수에 없는 키만 채움, 없으면 무시)")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "사용: llmgw [-config f] [-env f] call|serve|burst ...")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	loadEnv(*envPath)
	cfg, err := llmgw.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	gw, err := llmgw.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer gw.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	args := flag.Args()[1:]
	switch flag.Arg(0) {
	case "call":
		os.Exit(cmdCall(ctx, gw, args))
	case "serve":
		os.Exit(cmdServe(ctx, gw, cfg.Server.Addr))
	case "burst":
		os.Exit(cmdBurst(ctx, gw, args))
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func cmdCall(ctx context.Context, gw *llmgw.Gateway, args []string) int {
	fs := flag.NewFlagSet("call", flag.ExitOnError)
	route := fs.String("route", "", "라우트 이름")
	file := fs.String("file", "", "프롬프트를 파일에서 읽기")
	system := fs.String("system", "", "system 메시지")
	asJSON := fs.Bool("json", false, "JSON 응답 강제")
	fs.Parse(args)
	prompt := strings.Join(fs.Args(), " ")
	if *file != "" {
		b, err := os.ReadFile(*file)
		if err != nil {
			log.Print(err)
			return 1
		}
		prompt = strings.TrimSpace(prompt + "\n\n" + string(b))
	}
	if *route == "" || prompt == "" {
		log.Print("-route 와 프롬프트가 필요하다")
		return 2
	}
	req := llmgw.Request{Route: *route, JSON: *asJSON}
	if *system != "" {
		req.Messages = append(req.Messages, llmgw.Message{Role: "system", Content: *system})
	}
	req.Messages = append(req.Messages, llmgw.Message{Role: "user", Content: prompt})

	resp, err := gw.Call(ctx, req)
	if err != nil {
		log.Print(err)
		return 1
	}
	fmt.Println(resp.Content)
	fmt.Fprintf(os.Stderr, "\n--- provider=%s model=%s latency=%s tokens=%d/%d(reasoning %d)\n",
		resp.Provider, resp.Model, resp.Latency.Round(time.Millisecond), resp.Usage.PromptTokens,
		resp.Usage.CompletionTokens, resp.Usage.ReasoningTokens)
	for _, a := range resp.Attempts {
		status := "ok"
		if a.Error != "" {
			status = a.Error
		}
		fmt.Fprintf(os.Stderr, "    step%d %-10s %-6s wait=%dms call=%dms %s\n", a.Step, a.Provider, a.Reasoning, a.WaitedMS, a.LatencyMS, status)
	}
	return 0
}

func cmdServe(ctx context.Context, gw *llmgw.Gateway, addr string) int {
	srv := &http.Server{Addr: addr, Handler: gw.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Printf("llmgw listening on %s (routes: %s)", addr, strings.Join(sortedRoutes(gw), ", "))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Print(err)
		return 1
	}
	return 0
}

// cmdBurst 는 같은 라우트에 동시 N 건을 쏴서 프로바이더별 처리 분포·대기·폴백을 요약한다.
// 요청마다 번호를 붙여 동일 요청 합치기(singleflight)에 걸리지 않게 한다.
func cmdBurst(ctx context.Context, gw *llmgw.Gateway, args []string) int {
	fs := flag.NewFlagSet("burst", flag.ExitOnError)
	route := fs.String("route", "", "라우트 이름")
	n := fs.Int("n", 20, "동시 요청 수")
	fs.Parse(args)
	prompt := strings.Join(fs.Args(), " ")
	if *route == "" || prompt == "" {
		log.Print("-route 와 프롬프트가 필요하다")
		return 2
	}
	type row struct {
		provider string
		lat      time.Duration
		steps    int
		err      error
	}
	rows := make([]row, *n)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range *n {
		wg.Go(func() {
			t := time.Now()
			req := llmgw.Request{Route: *route, Messages: []llmgw.Message{{Role: "user", Content: fmt.Sprintf("[%d] %s", i, prompt)}}}
			resp, err := gw.Call(ctx, req)
			r := row{lat: time.Since(t), err: err}
			if err == nil {
				r.provider, r.steps = resp.Provider, len(resp.Attempts)
			}
			rows[i] = r
		})
	}
	wg.Wait()
	byProv := map[string][]time.Duration{}
	fails, fell := 0, 0
	for _, r := range rows {
		if r.err != nil {
			fails++
			continue
		}
		byProv[r.provider] = append(byProv[r.provider], r.lat)
		if r.steps > 1 {
			fell++
		}
	}
	fmt.Printf("burst route=%s n=%d wall=%s 실패=%d 폴백거친요청=%d\n", *route, *n, time.Since(start).Round(time.Millisecond), fails, fell)
	for p, ls := range byProv {
		slices.Sort(ls)
		fmt.Printf("  %-12s %3d건  p50=%s  max=%s\n", p, len(ls), ls[len(ls)/2].Round(time.Millisecond), ls[len(ls)-1].Round(time.Millisecond))
	}
	st, _ := json.Marshal(gw.Stats())
	fmt.Printf("  stats %s\n", st)
	if fails > 0 {
		return 1
	}
	return 0
}

func sortedRoutes(gw *llmgw.Gateway) []string {
	r := gw.Routes()
	sort.Strings(r)
	return r
}

// loadEnv 는 KEY=VALUE 파일에서 아직 환경변수에 없는 키만 채운다. 값은 출력하지 않는다.
func loadEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if _, set := os.LookupEnv(k); !set {
			os.Setenv(k, v)
		}
	}
	if err := sc.Err(); err != nil {
		log.Printf("env 파일 읽기: %v", err)
	}
}
