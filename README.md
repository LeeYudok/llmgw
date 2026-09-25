# llmgw — 설정 기반 LLM 게이트웨이

OpenAI 호환 엔드포인트 여러 개를 설정 파일(TOML 또는 YAML) 하나로 묶어 호출한다.
호출하는 쪽은 라우트 이름만 고르고, 순서·추론 여부·폴백·재시도·대기열은 설정이 정한다.

## 개념

- **provider**: 엔드포인트 하나. 분당 요청 한도(`rpm`), 동시 처리 수(`max_concurrency`), 대기열(`max_queue`,
  `queue_timeout`), 서킷 브레이커(`breaker_failures`, `breaker_cooldown`), 추론 전달 방식(`reasoning`)을 가진다.
- **route**: 호출 이름. `steps` 를 위에서부터 시도한다. 순서를 바꾸면 우선순위가 바뀐다.
- **step**: provider + 추론(`off|on|low|medium|high|inherit`) + `max_tokens` + `retries` + `max_retry_wait`.
  `inherit` 는 호출자가 보낸 `chat_template_kwargs.enable_thinking` 이나 `reasoning_effort` 를 따른다(없으면 off).
  `reasoning_effort` 의 `none` 은 off, `minimal` 은 low, `xhigh` 는 high 로 맞추고, 모르는 값은 지정 없음으로 본다.
  기존 앱이 기능마다 추론을 켜고 끄던 것을 게이트웨이 뒤에서도 그대로 유지할 때 쓴다.
- **client**: 게이트웨이를 부르는 서비스 하나. 서버 모드에서 Bearer 키로 식별하고, 허용 라우트(`routes`)·
  중계 허용 provider(`passthrough`)·자기 몫의 한도(`rpm`, `max_concurrency`, `max_queue`, `queue_timeout`)를 가진다.
  여러 서비스가 같은 외부 API 키를 나눠 쓸 때 provider 한도는 게이트웨이 한 곳에서 지키고,
  서비스별 한도로 한 서비스의 배치가 다른 서비스 몫까지 먹지 않게 한다.

## 요청에서 응답까지

요청은 여섯 단계를 거쳐 응답이 됩니다. 단계마다 조건이 맞지 않으면 옆으로 빠져 에러 응답이 됩니다.
캐시에 같은 요청의 결과가 있으면 step 체인을 건너뛰고 바로 응답합니다. 캐시와 같은 요청 합치기는 스트림이 아닌
요청에만 적용합니다(스트림은 아래 "사용"의 스트림 설명 참고).

![요청은 인증, 라우트·권한 확인, client 게이트, 캐시·중복 합치기, step 체인, 기록을 거쳐 응답이 된다](docs/flow-overview.svg)

step 하나는 LLM 하나입니다. 외부 차단, `spill`, 브레이커 열림, 대기열 가득 참, 4xx, 검증 실패는 기다리지 않고
바로 다음 step 으로 넘깁니다. 게이트에서 자리를 기다릴 때(`queue_timeout`, `max_wait`)와 429·5xx·네트워크 오류로
재시도할 때(백오프 또는 `Retry-After`, `max_retry_wait` 이하)는 그만큼 기다린 뒤에 넘깁니다. 개인정보 마스킹은
`[mask] external = true` 일 때 external provider 에, 또는 `mask = true` 인 provider 에 적용합니다.

![step 하나는 body 만들기, 외부 차단 확인, 마스킹, spill 판단, provider 게이트, HTTP 호출, 응답 검증을 거친다](docs/flow-step.svg)

LLM 마다 입구(게이트)가 하나씩 있습니다. 요청은 브레이커·대기열·동시 처리 슬롯·분당 한도를 차례로 통과해야
호출됩니다. `spill` 과 `max_wait` 가 쓰는 예상 대기 시간은 이 칸들의 상태로 계산합니다.

![요청은 브레이커, 대기열, 동시 처리 슬롯, 분당 한도를 차례로 지나야 호출된다](docs/flow-gate.svg)

## 호출이 몰릴 때

| 상황 | 동작 |
|---|---|
| 분당 한도 초과 | 요청 간격을 `1분/rpm` 으로 벌려 대기열에서 기다린다 |
| 대기열이 가득 참 | 기다리지 않고 다음 step 으로 넘긴다 |
| `queue_timeout` 동안 자리를 못 얻음 | 다음 step 으로 넘긴다 |
| step `spill = "auto"` 이고 다음 step 에서 더 빨리 끝날 것으로 보임 | 기다리지 않고 바로 다음 step 으로 넘긴다(아래 "빨리 넘기기") |
| step `max_wait` 보다 오래 기다릴 것으로 보임 | 기다리지 않고 바로 다음 step 으로 넘긴다 |
| 429·5xx·네트워크 오류 | `Retry-After`(없으면 0.5s·1s·2s 지수 백오프)만큼 기다렸다 재시도(분당 한도 자리가 대기 마감보다 뒤면 기다리지 않는다). `max_retry_wait` 보다 길면 기다리지 않고 다음 step. 429 는 같은 provider 를 쓰는 다른 요청도 같이 늦춘다 |
| 연속 실패 `breaker_failures` 회 | `breaker_cooldown` 동안 그 provider 를 건너뛴다. 쿨다운이 지나면 시험 요청 1건만 보내 성공하면 닫고, 실패하면 바로 다시 연다. 네트워크 오류·타임아웃·408·429·5xx 만 센다(호출자 취소, 400 같은 요청 오류, 응답 검증 실패는 세지 않는다) |
| 응답 검증 실패(빈 응답, JSON 아님, 필수 문구 없음, 너무 짧음) | 재시도 없이 다음 step |
| 같은 요청이 동시에 여러 번 | 한 번만 호출하고 결과를 나눠 준다. `cache_ttl` 이 있으면 그동안 재사용 |

추론을 켠 step 은 `max_tokens` 를 provider 의 `reasoning_min_tokens`(기본 4096) 이상으로 올린다.
추론 토큰과 본문 토큰이 한도를 공유해 본문이 빈 채로 잘리는 것을 막기 위해서다.

## 빨리 넘기기 — 기다릴지, 다음 LLM 으로 갈지

기본 동작은 앞 step 의 대기열이 꽉 차거나 `queue_timeout` 이 지나야 다음 step 으로 넘어간다. 그래서 다음 LLM 이
비어 있어도 앞에서 한참 기다리다 넘어가는 요청이 생긴다. 게이트웨이는 provider 마다 지금 들어온 요청이 얼마나
기다릴지 어림한다.

- 분당 한도: 이미 예약된 다음 발송 시각 + 앞에서 기다리는 요청 수 × 요청 간격
- 동시 처리: 앞에 밀린 요청 수 ÷ `max_concurrency` × 최근 평균 소요 시간

이 값으로 step 에서 두 가지 방식을 고를 수 있다(둘 다 마지막 step 에는 적용하지 않는다).

| 설정 | 동작 |
|---|---|
| `spill = "auto"` | 이 provider 의 예상 완료(대기 + 평균 소요)가 다음 step 보다 길면 바로 넘긴다. 부하에 따라 스스로 맞춰지므로 이쪽을 권한다. 두 provider 모두 소요 시간 기록이 있어야 동작한다(기록이 없으면 기존처럼 기다린다) |
| `max_wait = "3s"` | 예상 대기가 이 값보다 길면 바로 넘기고, 실제 대기도 이 값으로 자른다. 값이 너무 작으면 다음 step 에 몰려 오히려 느려진다 |

넘긴 시도는 `예상 대기 초과` 로 기록되고 `/stats` 의 `EarlySpills` 로 센다. `/stats` 는 provider 별로 최근 성공
호출 256건의 `LatencyP50MS`·`LatencyP95MS` 와 지금 들어오면 예상되는 대기 `EstWaitMS` 도 보여준다.

## 여러 서비스가 같이 쓸 때 (clients)

```toml
[clients.batch-worker]
api_key_env     = "LLMGW_KEY_BATCH"   # 이 서비스가 Bearer 로 보낼 키
routes          = ["fast"]            # 허용 라우트("*" = 전부)
max_concurrency = 4                   # 이 서비스의 동시 요청 수
max_queue       = 32                  # 넘치면 즉시 429 (Retry-After: 5)
```

| 응답 | 의미 |
|---|---|
| 401 | 키 없음·틀림 |
| 403 | 허용되지 않은 라우트 또는 passthrough |
| 429 | 클라이언트 한도 초과 |
| 502 | 모든 step 실패(`attempts` 에 시도 기록) |

clients 를 정의했으면 인증 없이는 열리지 않는다. `serve` 는 클라이언트의 `api_key_env` 가 하나라도 비어 있으면
기동하지 않는다(라이브러리는 `Gateway.CheckClientKeys` 로 확인).

`/v1/models` 는 호출한 클라이언트가 쓸 수 있는 라우트만 보여준다. `/stats` 는 provider·client 별
호출 수·성공·실패·거절·토큰·대기 중 요청 수를 돌려준다. `log_path` 를 주면 요청 1건당 JSONL 한 줄
(클라이언트, 라우트, 처리 provider, 지연, 토큰, 에러)을 남긴다. 프롬프트·응답 본문은 남기지 않는다.

## 외부 provider 차단

조직 밖으로 데이터가 나가는 provider 에 `external = true` 를 붙인다. 다음 경우 그 provider 의 step 은
호출하지 않고 건너뛴다(시도 기록에 `외부 provider 차단` 으로 남는다). 내부 step 이 모두 실패하면 외부로
폴백하지 않고 에러를 돌려준다.

- 클라이언트에 `allow_external = false`
- 요청 헤더 `X-LLMGW-No-External: 1` (라이브러리는 `Request.NoExternal = true`)

passthrough 도 같은 규칙을 따른다(차단이면 403). passthrough 경로에 `.`·`..`(인코딩 포함)이 있으면 400 으로 거절한다.

## 개인정보 마스킹

`[mask] external = true` 면 external provider 로 보내는 요청의 메시지에서 개인정보·비밀값을 `[REDACTED:<종류>]`
로 바꿔 보낸다. 내부 provider 로는 원문 그대로 보낸다. external 이 아니어도 provider 에 `mask = true` 를 주면 가린다.

| 종류 | 대상 |
|---|---|
| `rrn` | 주민등록번호·외국인등록번호 (YYMMDD-[1-8]NNNNNN, 하이픈 생략 포함) |
| `card` | 16자리 카드번호 (Luhn 검사 통과) |
| `phone` | 휴대전화·유선전화 |
| `account` | 하이픈으로 나뉜 계좌번호(숫자 10~16자리). Luhn 이 맞지 않는 4-4-4-4 숫자열도 여기서 가린다 |
| `email` | 이메일 주소 |
| `secret` | `sk-…`, `up_…`, AWS access key, `Bearer …` 토큰 |

`kinds` 로 쓸 종류를 고르고, `[mask.custom]` 에 이름 → 정규식(RE2)으로 패턴을 더한다. 가린 원문은 어디에도
남기지 않으며 되돌릴 수 없다. 시도 기록의 `masked` 와 응답 헤더 `X-LLMGW-Masked` 에 가린 건수가 남는다.
정규식 기반이라 형식이 다른 개인정보(이름, 주소, 하이픈 없는 계좌번호 등)는 잡지 못한다.
passthrough 요청 본문은 형식을 알 수 없어 가리지 않는다.

## 에러 메시지

호출자에게 돌려주는 에러와 시도 기록에는 `HTTP 500`, `연결 실패`, `타임아웃` 같은 분류만 담는다. upstream 응답
본문이나 내부 주소는 내보내지 않고 요청 로그(`log_path`)와 CLI 출력에만 남긴다. 라이브러리에서는
`Attempt.Detail`, `ChainError.Detail()` 로 원문을 볼 수 있다.

## response_format

호출자가 보낸 `response_format` 을 그대로 받는다. `json_schema`(strict 포함)는 provider 에
`json_schema = true` 일 때만 그대로 보내고, 아니면 `json_object` 로 낮춰 보낸다. 어느 쪽이든 `json_*`
형식을 요청했으면 응답이 JSON 인지 게이트웨이가 검증하고, 아니면 다음 step 으로 넘긴다.

`json_object` 로 낮춰 보낼 때는 스키마를 system 지시문으로 요청 앞에 붙여 모델이 형식을 따르게 하고,
응답이 스키마의 최상위 `required` 키와 `enum` 값을 지키는지 확인한다(전체 JSON Schema 검증기는 아니다).
어기면 다음 step 으로 넘긴다.

## passthrough — OpenAI 형식이 아닌 API

provider 에 `passthrough = true` 를 주면 `/passthrough/<provider>/<path>` 로 들어온 요청을 그대로
`<base_url>/<path>` 에 넘긴다(메서드·본문·쿼리·Content-Type 유지). 인증 헤더만 provider 키로 바꾸고,
provider 의 분당 한도·동시 처리·대기열과 클라이언트 한도를 chat 호출과 똑같이 센다. 문서 파싱처럼
같은 외부 키를 쓰는 전용 API 를 한도 관리 안에 넣을 때 쓴다. 클라이언트는 `passthrough` 목록에
provider 가 있어야 한다.

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

응답의 `llmgw` 필드에 실제 처리한 provider·캐시 여부·시도 기록이 들어 있다. 같은 내용을 응답 헤더
`X-LLMGW-Provider`, `X-LLMGW-Model`, `X-LLMGW-Attempts`, `X-LLMGW-Cached`, `X-LLMGW-Masked` 로도 준다.
스트림 응답은 본문에 `llmgw` 필드를 넣을 수 없어 헤더로만 알 수 있다.

`"stream": true` 면 provider 의 SSE 를 그대로 흘려보낸다. 첫 바이트를 받기 전의 실패(연결 오류·429·5xx)는
평소처럼 재시도·폴백하고, 스트림이 시작된 뒤 끊기면 다른 provider 로 이어붙이지 않는다(부분 응답을 섞지 않는다).
스트림 응답에는 검증·캐시를 적용하지 않는다. 호출자가 느리게 읽는 시간은 idle 로 세지 않는다. provider 에 `stream_usage = true` 를 주면
`stream_options.include_usage` 를 붙여 토큰 사용량을 받아 통계·로그에 남긴다(마지막에 choices 가 빈 청크가 하나 더 온다). 라이브러리는 `Gateway.Stream`(시작 시점을 알고 싶으면 `StreamWithStart`)을 쓴다.
스트림에서 provider `timeout` 은 응답 헤더를 받을 때까지만 적용되고, 그 뒤로는 줄 사이 간격이
`stream_idle_timeout`(기본 60s)을 넘으면 끊는다. 스트림 전체 길이에는 제한이 없어 긴 추론 응답도 잘리지 않는다.

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

같은 구성을 시간만 1/10 로 줄인 가짜 서버(upstage 0.25s·rpm 500·동시 8·대기열 16·queue_timeout 1.5s,
qwen 0.7s·동시 12)로 모의 실험한 결과(두 번 반복해 같은 경향). 두 provider 모두 소요 시간 기록이 있는 상태다.

| 동시 요청 | 설정 | 전체 | 평균 | upstage / qwen |
|---|---|---|---|---|
| 30 | 기본 | 2.3s | 1.10s | 13 / 17 |
| 30 | `max_wait = "1s"` | 1.6s | 1.00s | 9 / 21 |
| 30 | `spill = "auto"` | 1.6s | 0.99s | 10 / 20 |
| 60 | 기본 | 3.2s | 1.70s | 13 / 47 |
| 60 | `max_wait = "1s"` | 3.8s | 1.84s | 9 / 51 |
| 60 | `spill = "auto"` | 3.1~3.2s | 1.70s | 13 / 47 |

30건에서는 upstage 에서 1.5초 기다리다 넘어가던 요청이 바로 qwen 으로 가서 전체가 31% 줄었다. 60건에서는
qwen 도 붐벼 기다리는 편이 낫고, 고정 `max_wait` 는 qwen 에 더 몰아 오히려 느려진 반면 `auto` 는 기본과 같았다(차이는 측정 오차 범위).

## 한계

- 동일 요청 합치기는 기다리는 호출자가 한 명이라도 있는 동안 호출을 이어 가고, 모두 취소해야 upstream 호출도 취소한다.
  호출자의 deadline 은 각자에게만 적용되고 공유 호출 자체는 provider `timeout` 으로 끊긴다.
- 응답 캐시는 최대 1,024건이다. 넘치면 만료된 항목을, 그래도 넘치면 오래된 항목부터 버린다(건수 기준이며 바이트 크기는 세지 않는다).
- 캐시·통계는 프로세스 메모리에만 있다. 여러 인스턴스를 띄우면 한도도 인스턴스마다 따로 센다.
