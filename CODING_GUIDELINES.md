# Fluss-Go 코드 작성 규칙

이 문서는 Fluss-Go 프로젝트의 코드 작성, 변경, 검토에 적용하는 기본 원칙을 정의한다.
사람과 자동화 도구를 포함한 모든 기여자는 이 원칙을 따른다.

규칙이 서로 충돌하면 정확성, 데이터 정합성, 보안, 호환성을 우선한다. 공식 Apache Fluss
프로토콜 명세, 프로젝트의 공개 API 계약, 테스트로 확인되는 기존 동작을 판단의 근거로
사용한다.

## 프로젝트 표준 도구

| 목적 | 표준 도구 | 규칙 |
| --- | --- | --- |
| 언어와 빌드 | Go 1.25 이상 | 최소 버전은 `go.mod`에 명시하고 CI는 최신 Go 1.25 및 1.26 patch release를 사용한다. |
| 작업 실행 | [Task](https://taskfile.dev/) (`go-task`) v3.51.1 | 반복 가능한 로컬·CI 작업의 단일 진입점으로 사용한다. |
| 코드 포맷 | `gofmt` | 모든 Go 소스에 적용한다. |
| 정적 분석 | `go vet` | 추가 linter는 도입 전에 필요성과 의존성을 검토한다. |
| 취약점 검사 | `govulncheck` v1.6.0 | 표준 라이브러리와 Go module의 도달 가능한 알려진 취약점을 검사한다. |
| 저장소 보안 검사 | [Trivy](https://trivy.dev/) v0.72.0 | 의존성 취약점, secret, misconfiguration과 license를 검사한다. |
| 의존성 검토 | GitHub Dependency Review | PR에서 새로 도입되는 취약한 의존성을 차단한다. |
| 테스트 | `go test` | unit, race, fuzz와 integration 검증을 목적별 task로 제공한다. |
| 공개 API 문서 | `pkg.go.dev`, `go doc` | package manual, exported API comment와 실행 가능한 example을 검증한다. |
| 버전 관리 | Git과 GitHub | 모든 변경은 기능 브랜치와 PR로 관리한다. |

### Task 규칙

- 저장소 루트의 `Taskfile.yml`을 프로젝트 작업 명령의 source of truth로 사용한다.
- Taskfile은 공식 version 3 schema를 사용하고, 사용하는 기능에 필요한 최소 Task CLI
  version을 명시한다.
- Task CLI는 v3.51.1로 고정하고 로컬 설치 문서와 CI에서 동일하게 사용한다.
- formatter, code generation, test, race test, fuzz, integration test, lint와 CI 검증은
  이름이 명확한 task로 제공한다.
- 기본 task에는 사용 가능한 주요 task와 설명을 확인할 수 있는 동작을 제공한다.
- CI는 Taskfile의 task를 호출하며 동일한 명령을 workflow 파일에 중복해서 작성하지
  않는다.
- 문서와 PR 검증 절차는 직접적인 하위 명령보다 `task <name>`을 표준 명령으로 안내한다.
- 개별 하위 명령은 디버깅을 위해 직접 실행할 수 있지만, 반복되는 절차는 Taskfile에
  반영한다.
- Taskfile이 단순한 명령 하나를 불필요하게 복잡한 shell script로 감싸지 않도록 한다.
- 인증 정보와 환경별 비밀값을 Taskfile에 기록하지 않는다. 필요한 환경 변수는 이름,
  필수 여부와 기본값 정책을 문서화한다.

## 1. 기존 구현을 먼저 확인한다

- 새로운 코드를 작성하기 전에 프로젝트 내부의 기존 구현, 공통 모듈, 유틸리티와
  의존성을 먼저 확인한다.
- 동일하거나 유사한 기능이 있다면 새로 구현하지 않고 재사용하거나 확장한다.
- 기존 구현의 정확성, 유지보수 상태, 테스트 범위와 현재 요구사항의 적합성을 함께
  검토한다.
- 직접 구현이 필요하다면 표준 라이브러리, 기존 구현 또는 검증된 외부 라이브러리로
  해결하기 어려운 이유가 분명해야 한다.
- Apache Fluss의 공식 Java, Rust 및 다른 클라이언트 구현은 동작을 이해하기 위한
  참고 자료로 사용하되, 언어에 맞지 않는 구조를 그대로 옮기지 않는다.

## 2. 의존성을 신중하게 선택한다

기능을 구현할 때 다음 순서로 대안을 검토한다.

1. Go 표준 라이브러리
2. 프로젝트가 이미 사용하는 의존성
3. 작고 안정적으로 유지보수되는 외부 라이브러리
4. 프로젝트 내부 구현

이 순서는 기계적인 우선순위가 아니다. 작은 기능 하나를 위해 큰 의존성을 추가해야
한다면 범위가 명확한 내부 구현이 더 적합할 수 있다.

- 새로운 의존성을 추가할 때는 유지보수 상태, 릴리스 주기, 라이선스, 알려진 보안 문제,
  전이 의존성, 바이너리 크기와 프로젝트 적합성을 검토한다.
- 의존성 버전은 재현 가능한 빌드를 보장하도록 관리한다.
- 프로젝트 라이선스와 호환되지 않는 라이브러리를 추가하지 않는다.
- 사용하지 않는 의존성과 대체된 의존성은 관련 작업 범위 안에서 제거한다.

### Build-vs-buy 판정

다음 세 가지 판정을 구분하고 리뷰에서 명시한다.

1. **재사용 필수**: 표준 라이브러리, 기존 의존성 또는 안정적으로 유지보수되는
   라이브러리가 요구사항, 호환성, 보안과 크기 제약을 합리적으로 충족하면 이를
   재사용한다. 같은 동작을 프로젝트 내부에 다시 구현하지 않는다.
2. **새 의존성 승인**: 기존 도구로 해결할 수 없고 외부 라이브러리가 가장 적합하면
   구현 전에 의존성 도입을 제안한다. API를 프로젝트 경계에 직접 퍼뜨리기보다 얇은
   adapter로 감싸고 교체 비용을 제한한다.
3. **내부 구현 승인**: 적합한 라이브러리가 없거나, 정확한 Fluss 호환 동작이
   필요하거나, 사소한 동작에 비해 의존성 비용이 지나치게 큰 경우에만 범위가 제한된
   내부 구현을 허용한다.

단지 "의존성을 늘리고 싶지 않다"는 이유만으로 내부 구현을 선택할 수 없다. 내부
구현이나 새 의존성을 선택하는 PR 또는 연결된 설계 문서는 다음 근거를 포함해야 한다.

- 조사한 표준 라이브러리, 기존 구현과 외부 라이브러리 후보
- 각 후보가 요구사항에 맞지 않는 구체적인 이유
- 선택안의 유지보수 책임, 예상 변경 빈도와 소유 범위
- 라이선스, 알려진 취약점, 전이 의존성과 공급망 영향
- Fluss 및 Go 지원 버전, 공개 API와 binary size에 미치는 영향
- 대체 구현과 비교할 golden, compatibility, integration 또는 benchmark 검증

근거는 검토자가 같은 결론을 재현할 수 있을 정도로 구체적이어야 한다. 라이브러리
동작의 일부만 필요하면 코드를 복사하거나 유사 구현을 만들지 않고 공개 API 위에 얇은
adapter를 작성한다. upstream 코드를 vendoring 또는 파생 구현해야 한다면 라이선스,
NOTICE, 업데이트 방법과 보안 패치 책임을 함께 기록한다.

### 직접 구현 금지와 예외

- 암호 알고리즘, 난수 생성, 인증서 검증과 TLS 구현은 직접 작성하지 않는다. Go
  `crypto`, `crypto/tls` 또는 별도로 승인된 검증 라이브러리를 사용한다.
- 표준 압축, checksum, serialization과 범용 encoding 알고리즘은 표준 라이브러리나
  검증된 기존 의존성을 사용한다.
- 보안 검증을 우회하거나 암호·TLS 구현을 직접 해야만 하는 protocol 요구사항은
  기본적으로 지원 불가로 판단한다. 불가피하다는 공식 명세 근거와 별도 보안 승인이
  없는 예외를 허용하지 않는다.
- Fluss 고유 wire layout과 record codec, Java 클라이언트와 byte 단위로 같아야 하는
  hashing, 그리고 사소한 동작에 비해 의존성 및 전이 의존성이 지나치게 큰 경우는
  내부 구현 후보가 될 수 있다. 이 경우에도 primitive는 표준 구현을 재사용하고
  golden, malformed input, fuzz와 live compatibility test를 위험에 맞게 제공한다.

현재 내부 구현과 의존성 선택의 근거는
[`docs/build-vs-buy.md`](docs/build-vs-buy.md)에 기록한다. 새로운 예외를 추가하거나
기존 판단의 전제가 바뀌면 해당 문서를 같은 PR에서 갱신한다.

## 3. 단순하고 명확한 코드를 작성한다

- 정확성, 안정성, 가독성을 우선한다.
- 가능한 한 단순하고 이해하기 쉬운 구조를 선택한다.
- 의미 없는 추상화, 불필요한 계층과 타입, 미래 요구사항만을 위한 확장 지점을 만들지
  않는다.
- 중복 제거가 오히려 결합도나 복잡도를 높인다면 억지로 공통화하지 않는다.
- 실제 병목이 확인되지 않은 상태에서 가독성과 유지보수성을 희생하지 않는다.
- 명백히 비효율적인 알고리즘, 불필요한 할당과 반복 처리는 피한다.
- 주석은 코드만으로 드러나지 않는 이유, 제약과 프로토콜 의미를 설명한다. 코드가 하는
  일을 그대로 반복하지 않는다.

## 4. 작업 범위 안에서는 적극적으로 개선한다

현재 작업과 직접 관련된 다음 변경은 공개 동작을 바꾸지 않는 범위에서 함께 수행할 수
있다.

- 수정한 코드에서 발견한 명백한 버그
- 관련된 작은 중복과 불필요한 복잡성
- 오해를 유발하는 이름
- 누락된 입력 검증과 오류 처리
- 누락된 회귀 테스트
- 측정 또는 코드 구조로 명확하게 확인되는 성능 문제

다음 변경은 구현 전에 제안하고 합의한다.

- 공개 API, 데이터 형식 또는 설정 의미의 변경
- 새로운 외부 의존성 도입
- 패키지 경계 또는 주요 아키텍처 변경
- 대규모 파일 이동이나 리팩터링
- 호환성, 재시도, 보안 또는 운영 정책 변경
- 요청 범위를 실질적으로 확대하는 작업

관련 없는 코드까지 정리하기 위해 작업 범위를 넓히지 않는다.

## 5. 최소한이 아니라 필요한 만큼 변경한다

- 요청한 기능을 안전하고 완전하게 구현하는 데 필요한 범위를 수정한다.
- 변경량 자체를 최소화하는 것보다 올바른 구현과 안정성을 우선한다.
- 관련 없는 코드와 생성 파일, 메타데이터를 함께 변경하지 않는다.
- 기존 프로젝트의 패키지 구조, 코딩 스타일과 소유권 경계를 존중한다.
- 사용자가 작성 중인 변경이나 현재 작업과 무관한 변경을 되돌리지 않는다.

## 6. 확인 가능한 근거를 사용한다

- 코드, 테스트, 공식 문서, 프로토콜 명세와 설정으로 확인 가능한 내용은 먼저 확인한다.
- 요구사항, 기존 동작, 데이터 형식과 비즈니스 규칙을 근거 없이 가정하지 않는다.
- 결과, 공개 동작, 데이터 정합성 또는 호환성에 영향을 주는 불확실성은 임의로 결정하지
  않는다.
- 위험이 낮고 되돌릴 수 있는 구현 판단은 기존 프로젝트 관례를 따를 수 있다. 중요한
  가정과 그 영향은 작업 결과에 명시한다.
- 확인되지 않은 내용을 숨은 fallback이나 암묵적인 정책으로 구현하지 않는다.
- 확인할 수 없는 내용은 불확실한 상태와 필요한 추가 검증을 명확히 밝힌다.

## 7. 오류를 숨기지 않는다

- 오류를 성공처럼 처리하거나 조용히 무시하지 않는다.
- fallback은 명시된 정책과 검증 가능한 조건이 있을 때만 사용한다.
- 복구 가능한 오류, 재시도 가능한 오류와 영구 오류를 구분한다.
- 호출자가 `errors.Is`와 `errors.As`로 대응할 수 있도록 원인을 보존하고 필요한 연산
  정보를 추가한다.
- 프로토콜 오류 코드와 부분 실패 정보를 임의로 하나의 일반 오류로 축약하지 않는다.
- 인증 정보, 토큰, 비밀번호와 전체 요청 데이터 등 민감한 값을 오류나 로그에 포함하지
  않는다.
- 라이브러리 내부에서 오류를 기록한 뒤 같은 오류를 반환하여 중복 로그를 만들지 않는다.
  로깅 책임과 오류 반환 책임을 명확히 구분한다.

## 8. 신뢰 경계에서 입력을 검증한다

- 외부 입력, 네트워크 데이터와 설정값은 시스템 경계에서 검증한다.
- 내부에서는 검증된 불변 조건을 명확히 유지하며, 신뢰 경계가 다시 바뀌는 경우에만
  재검증한다.
- 빈 값, 잘못된 형식, 범위 초과, 정수 오버플로, 중복 요청, 잘못된 상태 전이와 부분
  실패를 고려한다.
- 잘못된 입력을 호출자에게 알리지 않고 임의로 보정하지 않는다.
- 배열과 버퍼를 할당하기 전에 길이와 개수의 상한을 검증한다.
- 데이터 정합성, 동시 접근과 취소 시점의 상태 변화를 항상 고려한다.

## 9. 위험에 비례하여 테스트한다

- 사용자 동작이나 프로그램 로직이 변경되면 관련 테스트를 추가하거나 수정한다.
- 정상 경로뿐 아니라 변경으로 발생할 가능성이 높은 예외와 경계 조건을 검증한다.
- 빈 값, 잘못된 입력, 최대·최소값, 타임아웃, 취소, 외부 시스템 실패, 부분 실패와
  동시성 문제를 변경 위험에 맞게 테스트한다.
- 버그 수정에는 가능한 한 수정 전 실패하고 수정 후 통과하는 회귀 테스트를 추가한다.
- 테스트 개수나 커버리지 수치보다 실제 오류 가능성과 회귀 위험을 우선한다.
- 테스트를 통과시키기 위해 검증 수준을 낮추거나 유효한 테스트를 삭제하지 않는다.
- 테스트 작성이나 실행이 현실적으로 불가능하면 이유, 수행한 대체 검증과 남은 위험을
  작업 결과에 명시한다.

### 필수 테스트 기준

- 구현 PR은 새로 추가하거나 변경한 동작의 테스트를 같은 PR에 포함해야 한다.
- 공개 API의 정상 동작, 주요 오류, 취소, timeout과 자원 정리 경로를 검증한다.
- 버그 수정은 회귀 테스트 없이 완료한 것으로 간주하지 않는다. 재현이 불가능하다면
  원인과 대체 검증을 PR에 명시하고 합의한다.
- wire format, parser와 codec 변경은 byte-level golden test와 malformed input test를
  포함해야 한다.
- 외부 입력을 해석하는 parser와 decoder에는 지속적으로 실행 가능한 fuzz target을
  제공한다.
- goroutine, channel, connection, callback, cache와 비동기 상태를 변경하면 race test와
  종료·취소 테스트를 포함해야 한다.
- 사용자에게 공개되는 Fluss workflow는 unit test만으로 완료하지 않고 Apache Fluss
  0.9.1 cluster를 사용하는 integration test로 검증한다.
- 실행 환경 때문에 필수 integration 또는 race test를 수행하지 못한 PR은 검증 누락
  상태를 명시하고, 이를 보완할 후속 조건 없이 merge하지 않는다.
- 생성 코드는 생성 결과 자체의 line coverage보다 generator test, deterministic
  regeneration과 representative golden test로 검증한다.

### 계층별 테스트

| 대상 | 필수 검증 |
| --- | --- |
| `fmsg`와 protocol codegen | unit, byte-level golden, malformed input, fuzz, deterministic regeneration |
| frame과 transport | unit, `net.Pipe` 또는 통제된 test server, timeout/cancel, race, fuzz |
| metadata와 routing | table-driven unit, refresh coalescing, stale metadata, partial failure, race |
| schema, Row, key와 Arrow codec | round-trip, golden, boundary, malformed input, fuzz, benchmark |
| writer | batching, bucket assignment, backpressure, retry/idempotence, cancel/close, race, integration |
| scanner와 lookup | ordering, offset, projection, partial failure, retry, cancel/close, race, integration |
| `fadm` | request mapping, typed result/error, partial failure, resource lifecycle integration |

### 테스트 품질

- 테스트는 구현 세부사항보다 외부 동작과 불변 조건을 검증한다.
- table-driven test가 가독성을 높이는 경우 적극적으로 사용하되 모든 경우를 하나의
  거대한 test 함수에 몰아넣지 않는다.
- 실제 시간 대기와 임의의 `sleep`에 의존하지 않는다. clock, dialer와 network 경계를
  주입하여 결정적으로 제어한다.
- 테스트는 서로 독립적이어야 하며 실행 순서와 다른 테스트의 전역 상태에 의존하지
  않는다.
- 실패 메시지는 어떤 입력과 불변 조건이 실패했는지 바로 알 수 있어야 한다.
- flaky test를 단순 재실행으로 숨기지 않고 원인을 수정한다.
- coverage는 누락을 찾는 보조 지표로 사용한다. 전체 수치만 맞추기 위한 테스트를
  작성하지 않으며, 변경된 핵심 분기와 오류 경로에 설명되지 않은 coverage 공백을
  남기지 않는다.
- 기준 구현이 쌓이면 package별 coverage baseline을 기록하고 정당한 이유 없이
  낮아지지 않게 한다.

### 표준 테스트 task

- `task test`: 전체 unit 및 golden test
- `task test:race`: 동시성 관련 package의 race test
- `task test:fuzz`: CI에서 실행 가능한 bounded fuzz smoke test
- `task test:integration`: Apache Fluss 0.9.1 integration test
- `task docs:check`: 공개 package manual과 canonical example 검증
- `task security`: `govulncheck`, Trivy와 의존성 무결성 검사를 포함하는 로컬 보안 gate
- `task verify`: PR 전에 필요한 formatter, generation, static analysis와 테스트 검증
- `task ci`: CI에서 사용하는 전체 필수 검증
- `task sonar`: release 준비 branch에서 PR 생성 전에 실행하고 Quality Gate 완료까지
  기다리는 로컬 검증

Release 준비 PR은 문서와 코드를 모두 완성한 뒤 `task ci`와 `task sonar`를 통과해야
생성한다. SonarQube Community Edition은 로컬 분석을 단일 `main` branch로 표시하므로
그 branch label은 판정에 사용하지 않는다. scanner가 보고한 SCM revision과 작업 branch의
`HEAD`가 같은지 확인하고 해당 revision의 Quality Gate 결과를 사용한다. Sonar finding은
merge 후가 아니라 같은 작업 branch에서 수정하며, Sonar를 post-merge GitHub Actions
gate로 취급하지 않는다.

## 10. 호환성과 보안을 기본값으로 삼는다

- 기존 공개 API, wire protocol, 데이터 구조와 설정의 호환성을 불필요하게 깨뜨리지
  않는다.
- 인증, 권한 검사와 TLS 검증을 우회하지 않는다.
- 민감한 정보를 코드, 테스트 fixture, 오류와 로그에 남기지 않는다.
- 재시도, 비동기 처리와 상태 변경은 멱등성, 순서 보장과 데이터 정합성을 고려한다.
- 호환성을 의도적으로 변경해야 한다면 영향 범위, 마이그레이션 방법과 폐기 일정을 먼저
  제안한다.
- 지원하는 Go 및 Apache Fluss 버전 범위를 문서화하고 테스트한다.

### CVE gate

- 모든 PR과 `main`의 CI에서 `task security`를 실행한다.
- `task security`는 `task security:go`, `task security:repo`와 module 무결성 검증을
  포함하며 `task verify`와 `task ci`의 필수 단계로 실행한다.
- `task security:go`는 `govulncheck ./...`를 실행한다.
- `task security:repo`는 Trivy filesystem scan을 실행한다.
- `govulncheck`는 v1.6.0으로 고정하고 Go 공식 vulnerability database를 사용한다.
- Trivy는 v0.72.0으로 고정하고 release checksum을 검증하여 설치한다.
- 지원하는 최신 Go 1.25 및 1.26 patch release에서 검사하여 표준 라이브러리 취약점도
  확인한다.
- 도달 가능한 알려진 취약점을 발견하면 severity와 관계없이 CI를 실패시킨다.
- Trivy는 Go module과 repository의 `HIGH`, `CRITICAL` 취약점 및 misconfiguration을
  차단하고 `MEDIUM` 이하는 report에 표시한다.
- Trivy secret scan에서 확인된 credential, token과 private key는 severity와 관계없이
  CI를 실패시킨다.
- Trivy license scan은 모든 의존성 license를 report한다. 허용·금지 license 정책이
  확정되면 forbidden 또는 restricted license를 gate에 포함한다.
- `govulncheck`와 Trivy 결과가 다르면 하나를 다른 하나의 대체 결과로 보지 않는다.
  호출 도달성, 데이터 출처와 검출 범위를 확인하여 각각 처리한다.
- GitHub Dependency Review를 모든 PR에서 실행하고 새로 도입되는 `moderate` 이상의
  알려진 취약점을 차단한다.
- runtime, development와 unknown scope의 의존성을 모두 검사한다. 개발 도구도 code
  generation과 release artifact를 변경할 수 있으므로 검사에서 제외하지 않는다.
- dependency graph와 Dependabot alert를 활성화하고 의존성 갱신을 정기적으로 확인한다.
- vulnerability database가 변경된 뒤에도 발견할 수 있도록 `main`에 대해 최소 주 1회
  scheduled security scan을 실행한다.
- GitHub Actions와 보안 도구의 version을 고정한다. GitHub Actions는 가능한 경우
  immutable commit SHA로 고정하고 제한된 권한으로 실행한다.
- CVE 또는 GHSA를 근거 없이 ignore하거나 검사 task를 건너뛰지 않는다.
- 즉시 해결할 수 없는 취약점의 임시 예외는 advisory ID, 영향 분석, 완화 조치, 담당자,
  추적 issue와 만료일을 기록하고 명시적인 승인을 받아야 한다.
- 보안 예외의 최대 유효 기간은 30일로 하며 만료 전에 수정하거나 다시 검토한다.
- 보안 검사 실패와 예외는 PR에 명확히 표시하며 일반 테스트 성공으로 대체하지 않는다.

## 11. 더 나은 방법을 근거와 함께 제안한다

- 현재 방식보다 정확성, 안정성, 성능, 유지보수성 또는 운영 측면에서 더 나은 방법이
  있다면 문제점과 개선 이유를 함께 제안한다.
- 장점, 단점, 영향 범위와 마이그레이션 비용을 설명한다.
- 현재 요구사항을 충족하는 구현과 장기적인 개선안을 구분한다.
- 현재 작업 범위 안에서 안전한 소규모 개선은 함께 반영할 수 있다.
- 대규모 리팩터링, 아키텍처 변경, 새로운 의존성 도입과 작업 범위 확대는 합의 없이
  적용하지 않는다.

## 12. 작업 결과를 명확하게 보고한다

작업 규모에 맞게 다음 내용을 보고한다.

- 변경한 주요 파일과 핵심 변경 사항
- 수행한 테스트, 정적 분석과 검증 결과
- 실행하지 못한 검증과 그 이유
- 남아 있는 위험 요소 또는 추가 검토 사항
- 현재 작업과 구분되는 장기 개선 제안

작은 변경에는 간결한 보고로 충분하다. 위험 요소나 추가 제안이 없다면 형식적인 내용을
만들어 내지 않는다.

## 13. Go 코드 규칙

- `gofmt`를 적용하고 Go의 일반적인 이름, 오류 처리와 패키지 설계 관례를 따른다.
- 패키지 이름은 짧고 의미가 명확해야 하며 중복되거나 불필요한 이름을 피한다.
- 인터페이스는 구현자가 아니라 사용하는 쪽에서 필요한 최소 메서드로 정의한다.
- 반환 타입을 추상화할 명확한 이유가 없다면 구체 타입을 반환한다.
- `context.Context`는 취소되거나 대기할 수 있는 공개 메서드의 첫 번째 인자로 전달한다.
  일반 설정이나 장기 상태를 보관하기 위해 구조체에 저장하지 않는다.
- 오류는 `%w`로 원인을 보존한다. 오류 문자열을 비교하거나 정상적인 제어 흐름에
  `panic`을 사용하지 않는다.
- 전역 변경 가능 상태와 암묵적인 초기화를 피한다.
- 공개 타입과 메서드는 동시 호출 가능 여부, 소유권과 수명 주기를 문서화한다.
- 각 공개 package는 전용 `doc.go`에 package의 목적, 주요 진입점과 사용 시 주의사항을
  설명한다.
- 직접 작성한 exported identifier에는 이름으로 시작하는 정확한 doc comment를 작성한다.
  생성 도구가 관리하는 소스는 수동으로 수정하지 않는다.
- 공개 API 문서에는 해당되는 경우 zero value의 유효성, 동시 호출 가능 여부, 자원
  소유권과 수명, `Close` 책임, context 취소와 부분 실패의 의미를 포함한다.
- 대표 사용법은 외부 test package의 canonical example로 작성하고 실제 cluster, network,
  credential에 의존하지 않게 한다. 출력 계약을 보여 줄 필요가 있는 예제만 `Output:`
  주석으로 실행한다.
- 공개 API나 사용법을 변경하면 package manual, example과 README의 온라인 문서 링크를
  함께 검토하고 `task docs:check`로 검증한다.
- callback이나 사용자 코드는 내부 lock을 잡은 상태에서 호출하지 않는다.
- channel을 닫는 책임은 channel을 생성하고 송신을 소유한 코드에 둔다.
- background goroutine에는 명확한 소유자와 종료 조건이 있어야 한다. `Close`, context
  취소와 오류 경로에서 goroutine과 connection이 누수되지 않아야 한다.
- `Close`의 중복 호출 정책과 `Flush` 중 취소·실패 시 의미를 공개 API 계약으로 정한다.
- 성능 최적화는 benchmark 또는 profiling 결과를 근거로 하며, 성능에 민감한 변경에는
  필요한 benchmark를 추가한다.

## 14. Fluss 프로토콜과 네트워크 규칙

### API와 패키지 경계

- `fmsg`는 wire request/response, API key와 version 계약만 소유한다. 연결 관리, 재시도,
  metadata cache와 사용자용 테이블 모델을 포함하지 않는다.
- `fgo`는 연결, metadata, table data operation과 사용자용 타입을 소유한다.
- `fadm`은 `fgo`의 연결과 request 기능을 재사용하며 별도의 connection pool을 만들지
  않는다.
- 내부 transport와 codec 타입을 공개 API로 노출하지 않는다.
- Apache Arrow 타입을 공개 API에 노출할 때는 Arrow-Go 버전 결합과 일반 Row API에
  미치는 영향을 먼저 검토한다.

### 공개 API와 설정 명명

- `fgo`와 `fadm`의 exported identifier, option, config field와 문서 용어는 지원하는
  Apache Fluss 버전의 공식 공개 개념과 이름을 우선한다. Fluss 용어가 존재하면 Kafka에서
  유래한 이름, wire 구현 세부사항 또는 의미가 넓은 로컬 별칭을 새로 만들지 않는다.
- 명명과 설정 의미의 source of truth는 고정된 Fluss 버전의 공식 public Java client 및
  admin API, `ConfigOptions`, 생성된 configuration reference 순서로 확인한다. Protocol
  schema는 wire 계약의 기준이며 사용자용 API 이름을 정당화하는 단독 근거로 사용하지
  않는다.
- 공개 API 또는 설정을 추가하거나 개편할 때는 관련 항목만 표본 확인하지 않고 해당
  영역의 upstream 공개 표면을 전수 대조한다. 직접 대응, Go에 맞춘 의미 차이, Fluss에는
  있지만 미지원인 항목과 Go 전용 확장을 각각 기록한다.
- Go 전용 transport, `context`, TLS termination, callback, resource safety option은 필요한
  경우 제공할 수 있다. 이 경우 공식 Fluss 기능이나 동일 설정의 직접 구현으로 오해되지
  않도록 차이와 지원 경계를 문서화한다.
- 설정 표면이 변경되면 [`docs/client-configuration.md`](docs/client-configuration.md), API
  baseline, example, migration guidance와 changelog의 영향을 같은 PR에서 검토한다.
- 생성된 `pkg/fmsg` 이름과 field는 pinned protocol 입력을 그대로 따르며 명명 통일을 위해
  손으로 수정하지 않는다. 사용자 친화적인 용어는 `fgo` 또는 `fadm` 경계에서 제공한다.

### 프로토콜 호환성

- Apache Fluss의 공식 protocol schema와 API key 정의를 source of truth로 사용한다.
- request마다 지원하는 최소·최대 API version을 명시하고 server와 협상한 범위 안에서
  가장 적절한 version을 사용한다.
- 지원하지 않는 version이나 필드를 조용히 다른 의미로 변환하지 않는다.
- request ID, response type, frame length와 message length를 읽기 전에 범위와 상한을
  검증한다.
- 알 수 없는 응답, 누락된 필수 필드와 trailing data의 처리 정책을 명시하고 테스트한다.
- coordinator와 tablet server의 역할을 구분하고 API 및 bucket metadata에 따라
  request를 routing한다.
- protocol 변경은 공식 Java 또는 Rust 클라이언트와 비교하고, 가능하면 동일 byte
  sequence를 확인하는 golden test를 추가한다.

### 코드 생성

- protocol 생성 코드는 직접 수정하지 않는다.
- 생성 파일에는 생성 코드임을 나타내는 header와 생성 도구 정보를 포함한다.
- 입력 schema, upstream Fluss version 또는 commit과 생성 도구 version을 고정한다.
- 누구나 동일한 결과를 만들 수 있도록 생성 명령과 필요한 도구를 문서화한다.
- 생성 결과의 차이는 코드 리뷰가 가능해야 하며, 관련 없는 schema 전체를 불필요하게
  다시 생성하지 않는다.

### 재시도와 부분 실패

- 재시도는 해당 API의 멱등성이 확인되거나 producer ID, sequence 등 중복 방지 수단이
  있을 때만 수행한다.
- 인증 실패, 잘못된 입력과 지원하지 않는 API version은 자동 재시도하지 않는다.
- 재시도는 context deadline과 취소를 존중하고, 최대 시도 횟수 또는 전체 시간 제한을
  가진다.
- 반복 재시도에는 제한된 exponential backoff와 jitter를 사용한다.
- metadata 오류로 재시도할 때는 필요한 metadata를 갱신하되 무제한 갱신 loop를 만들지
  않는다.
- table, partition과 bucket별 부분 성공 및 실패를 호출자가 확인할 수 있게 보존한다.
- 동일 bucket 안에서 요구되는 write 및 scan 순서를 깨뜨리지 않는다.

### 연결과 자원 관리

- frame과 response의 최대 크기를 설정할 수 있어야 하며, 기본값은 안전한 상한을 가진다.
- connection별 진행 중인 request와 request ID의 소유 관계를 명확히 한다.
- timeout 또는 취소된 request의 늦은 response가 다른 request와 연결되지 않게 한다.
- connection 실패 시 진행 중인 request를 빠짐없이 완료시키고 각 호출자에게 원인을
  반환한다.
- buffer pool을 사용할 때 큰 buffer가 장기간 보존되거나 다른 호출에서 유효 데이터로
  재사용되지 않게 한다.
- 사용자 callback, hook과 logger의 지연이 network read loop 전체를 막지 않도록 한다.

### 검증

- codec과 frame parser에는 malformed input, truncated input, 최대 크기와 정수
  오버플로 테스트를 작성한다.
- parser와 decoder에는 적절한 fuzz test를 추가한다.
- 동시성 또는 connection 수명 주기를 변경하면 가능한 범위에서 `go test -race`를
  실행한다.
- protocol golden test와 실제 Fluss cluster를 사용하는 integration test를 구분한다.
- integration test는 지원하는 Fluss version과 필요한 환경을 명확히 표시한다.

## 15. 모든 변경은 기능 브랜치와 PR로 관리한다

### 기본 원칙

- `main` 브랜치에서 직접 작업하거나 commit 및 push하지 않는다.
- 코드, 문서, 의존성, 설정과 생성 코드를 포함한 모든 변경은 별도 브랜치와 PR을 거친다.
- 하나의 브랜치는 하나의 명확한 목적만 가져야 한다. 관련 없는 변경을 같은 PR에
  포함하지 않는다.
- 작업 브랜치는 최신 `origin/main`을 기준으로 생성한다.
- 다른 작업자가 만든 branch나 commit을 임의로 수정하거나 덮어쓰지 않는다.

원격 저장소에 기준 브랜치가 전혀 없는 경우, `README.md`, 코드 작성 규칙과 module
definition으로 최초 `main`을 만드는 bootstrap commit만 일회성 예외로 허용한다. 기준
브랜치가 생성된 뒤에는 이 예외를 다시 적용하지 않는다.

### 브랜치 이름

브랜치 이름은 변경 종류와 목적이 드러나도록 다음 형식을 사용한다.

```text
feat/<feature>
fix/<bug>
docs/<topic>
refactor/<scope>
test/<scope>
chore/<task>
```

예:

```text
feat/fmsg-api-versions
feat/fgo-transport
fix/frame-length-validation
docs/package-design
refactor/metadata-cache
chore/update-protobuf
```

- 소문자 영문과 숫자, hyphen을 사용한다.
- 이슈 번호가 있다면 목적을 해치지 않는 범위에서 포함할 수 있다.
- `feature`, `temp`, `work`처럼 작업 내용을 알 수 없는 이름을 사용하지 않는다.

### 작업 흐름

모든 작업은 다음 흐름을 따른다.

1. `origin/main`의 최신 상태를 확인한다.
2. 작업 목적에 맞는 새 브랜치를 생성한다.
3. 구현과 함께 필요한 테스트와 문서를 작성한다.
4. formatter, test, 정적 분석과 필요한 호환성 검증을 수행한다.
5. 변경 범위와 검증 결과를 확인하고 논리적인 단위로 commit한다.
6. 원격에 브랜치를 push하고 PR을 생성한다.
7. 장기 작업이나 조기 피드백이 필요한 작업은 draft PR로 생성한다.
8. 리뷰 의견과 CI 결과를 반영한다.
9. 명시적인 승인 후 merge한다.
10. merge가 확인되면 작업 브랜치를 정리한다.

### Commit 규칙

- commit은 하나의 논리적인 변경을 표현하고 독립적으로 검토할 수 있어야 한다.
- commit message는 무엇을 변경했는지 명령형으로 간결하게 작성한다.
- 테스트를 통과시키기 위한 임시 변경, 디버깅 출력과 불필요한 생성물을 commit하지
  않는다.
- 이미 공개되거나 리뷰 중인 branch의 이력을 변경해야 한다면 협업 영향을 먼저
  확인한다.
- `main`에는 force push하지 않는다. 작업 branch에서 불가피하게 이력을 정리할 때는
  대상 branch를 확인하고 `--force-with-lease`를 사용한다.

### PR 규칙

PR에는 최소한 다음 내용을 포함한다.

- 변경 이유와 해결하려는 문제
- 핵심 변경 사항
- 수행한 테스트와 검증 결과
- 실행하지 못한 검증과 그 이유
- 공개 API, protocol, 설정과 호환성에 미치는 영향
- 알려진 위험과 후속 작업

- PR 크기는 한 번에 이해하고 검토할 수 있는 수준으로 유지한다.
- protocol 생성 결과처럼 기계적인 변경과 수동 구현은 가능하면 commit을 분리한다.
- 공개 API, wire protocol, 새로운 의존성 또는 주요 아키텍처 변경은 구현 전에 설계
  합의를 남긴다.
- CI 실패, 해결되지 않은 리뷰 의견과 확인되지 않은 호환성 문제가 있는 PR은 merge하지
  않는다.
- 기본 merge 방식은 squash merge로 한다. 여러 commit의 독립적인 이력을 보존해야 할
  명확한 이유가 있을 때만 다른 방식을 사용한다.
- PR 생성은 merge 승인을 의미하지 않는다. merge는 명시적인 승인 후 수행한다.

### Changelog 규칙

- 사용자에게 보이는 기능, 동작, 공개 API, 호환성, 보안 또는 운영 요구사항을 변경하는
  PR은 루트 `CHANGELOG.md`의 `Unreleased` 섹션을 같은 PR에서 갱신한다.
- 내부 리팩터링, 테스트만의 변경과 사용자 영향이 없는 문서 교정은 changelog에
  기록하지 않는다.
- 항목은 commit이나 파일 목록이 아니라 사용자가 받는 영향과 필요한 대응을 설명한다.
- 릴리스할 때 `Unreleased` 항목을 버전과 날짜가 있는 섹션으로 옮기고, 새
  `Unreleased` 섹션과 tag 비교 링크를 준비한다.
- GitHub 자동 릴리스 노트는 기여자와 PR 목록을 제공하는 보조 자료로 사용하며
  `CHANGELOG.md`를 대체하지 않는다.

### 공개 문서 검증 규칙

- `task docs:check`는 생성 코드와 테스트 파일을 제외한 공개 package, 선언, 메서드와
  interface 메서드의 GoDoc 누락을 검사한다.
- 공개 struct field는 이름만 반복하는 주석을 강제하지 않는다. 단위, 기본값, sentinel,
  소유권, 유효 조건, 부분 실패 또는 호환성 의미가 있는 field는 같은 type 또는 field
  주석으로 계약을 설명하고 review에서 확인한다.
- 생성 파일 제외는 파일명 패턴이 아니라 표준 `Code generated ... DO NOT EDIT.`
  marker를 기준으로 결정한다.
- 문서 검사는 파일, line, symbol과 실패 이유를 출력하여 바로 수정할 수 있어야 한다.
