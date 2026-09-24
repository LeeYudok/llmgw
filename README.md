# llmgw — 설정 기반 LLM 게이트웨이

OpenAI 호환 엔드포인트 여러 개를 설정 파일(TOML 또는 YAML) 하나로 묶어 호출한다.
호출하는 쪽은 라우트 이름만 고르고, 순서·추론 여부·폴백·재시도·대기열은 설정이 정한다.

## 개념

- **provider**: 엔드포인트 하나. 분당 요청 한도(`rpm`), 동시 처리 수(`max_concurrency`), 대기열(`max_queue`,
  `queue_timeout`), 서킷 브레이커(`breaker_failures`, `breaker_cooldown`), 추론 전달 방식(`reasoning`)을 가진다.
- **route**: 호출 이름. `steps` 를 위에서부터 시도한다. 순서를 바꾸면 우선순위가 바뀐다.
- **step**: provider + 추론(`off|on|low|medium|high`) + `max_tokens` + `retries` + `max_retry_wait`.

## 호출이 몰릴 때

| 상황 | 동작 |
|---|---|
| 분당 한도 초과 | 요청 간격을 `1분/rpm` 으로 벌려 대기열에서 기다린다 |
| 대기열이 가득 참 | 기다리지 않고 다음 step 으로 넘긴다 |
| `queue_timeout` 동안 자리를 못 얻음 | 다음 step 으로 넘긴다 |
| 429·5xx·네트워크 오류 | `Retry-After`(없으면 0.5s·1s·2s 지수 백오프)만큼 기다렸다 재시도. `max_retry_wait` 보다 길면 기다리지 않고 다음 step. 429 는 같은 provider 를 쓰는 다른 요청도 같이 늦춘다 |
| 연속 실패 `breaker_failures` 회 | `breaker_cooldown` 동안 그 provider 를 건너뛴다 |
| 응답 검증 실패(빈 응답, JSON 아님, 필수 문구 없음, 너무 짧음) | 재시도 없이 다음 step |
| 같은 요청이 동시에 여러 번 | 한 번만 호출하고 결과를 나눠 준다. `cache_ttl` 이 있으면 그동안 재사용 |

추론을 켠 step 은 `max_tokens` 를 provider 의 `reasoning_min_tokens`(기본 4096) 이상으로 올린다.
추론 토큰과 본문 토큰이 한도를 공유해 본문이 빈 채로 잘리는 것을 막기 위해서다.

## 추론 전달 방식 (`provider.reasoning`)

| 값 | 요청에 싣는 것 | 대상 |
|---|---|---|
| `chat_template` | `chat_template_kwargs.enable_thinking: true/false` | vLLM Qwen·EXAONE |
| `effort` | `reasoning_effort: low/medium/high` (off 면 안 보냄, `on` = medium) | Upstage, OpenAI |
| `none` | 아무것도 안 보냄 | 추론 없는 모델 |

## 사용

```bash
go install github.com/leeyudok/llmgw/cmd/llmgw@latest
# 또는 git clone 후: go build -o llmgw ./cmd/llmgw

./llmgw -config llmgw.example.toml call -route news-deep -file article.txt
./llmgw -config llmgw.example.toml call -route fast -json '... JSON 만'
./llmgw -config llmgw.example.toml burst -route news-deep -n 30 "프롬프트"   # 부하 분산 확인
./llmgw -config llmgw.example.toml serve                                 # 127.0.0.1:17902
```

서버 모드는 OpenAI 호환이다. `model` 에 라우트 이름을 넣는다.

```bash
curl -s localhost:17902/v1/chat/completions -d '{"model":"news-deep","messages":[{"role":"user","content":"..."}]}'
curl -s localhost:17902/stats
```

응답의 `llmgw` 필드에 실제 처리한 provider·캐시 여부·시도 기록이 들어 있다. `stream` 은 지원하지 않는다.

키는 provider 의 `api_key_env` 환경변수에서 읽는다. `-env` 로 지정한 KEY=VALUE 파일은 환경변수에 없는 키만 채운다.

라이브러리로도 쓸 수 있다:

```go
cfg, _ := llmgw.LoadConfig("llmgw.toml")
gw, _ := llmgw.New(cfg)
resp, err := gw.Call(ctx, llmgw.Request{Route: "news-deep", Messages: []llmgw.Message{{Role: "user", Content: text}}})
```

## 예시 측정

예시 측정 — `llmgw.example.toml` 구성(자체 호스팅 vLLM + Upstage solar-pro4)에서 `news-deep` 라우트에 동시 30건:

| 처리 | 건수 | p50 | 최대 |
|---|---|---|---|
| upstage(solar-pro4, rpm 50) | 13 | 8.3s | 15.1s |
| qwen(추론 켬, 폴백) | 17 | 7.5s | 19.2s |

실패 0건, 전체 19.2초. Upstage 대기열이 차서 13건은 즉시, 4건은 15초 대기 초과로 Qwen 에 넘어갔다.

## 한계

- 동일 요청 합치기는 먼저 들어온 요청의 컨텍스트로 호출한다. 그 호출자가 취소하면 뒤에 붙은 요청도 같은 에러를 받는다.
- 캐시·통계는 프로세스 메모리에만 있다. 여러 인스턴스를 띄우면 한도도 인스턴스마다 따로 센다.
