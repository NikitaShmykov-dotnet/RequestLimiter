# RequestLimiter

RequestLimiter — локальный HTTP-шлюз перед симулятором backend. У каждого тестового клиента отдельная квота на каждом маршруте. Например, всплеск запросов с ключом `demo-aggressive` может заполнить его очередь на `/work`, а токены и места клиента `demo-normal` останутся доступны.

## Запуск

Нужен [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0). Версия для сборки зафиксирована в `global.json`. Команды выполняются из каталога `FairGate`.

```sh
dotnet restore
dotnet build -c Release --no-restore
```

В двух терминалах запустите backend и шлюз соответственно.

```sh
dotnet run --project src/Backend -c Release --no-build
```

```sh
dotnet run --project src/RequestLimiter -c Release --no-build
```

```sh
curl -i -H "X-Api-Key: demo-normal" -H "Content-Type: text/plain" --data hello "http://127.0.0.1:5070/work?delayMs=80"
```

Шлюз вернёт `200` и `{"kind":"work","delayMs":80,"bytes":5}` от симулятора на порту 5071. В Windows PowerShell используйте `curl.exe`. Другие запросы есть в [requests.http](requests.http). Состояние процесса доступно на `/health`, описание API на `/openapi/v1.json`, счётчики на `/metrics`. Там `accepted` означает допуск к проксированию, `rejected` складывает отказы по частоте и очереди, `queued` считает входы в очередь, а `inFlight` показывает текущие занятые слоты проксирования.

## Квоты

| API-ключ | Маршрут | Burst | Токенов/с | Слоты | Очередь |
| --- | --- | ---: | ---: | ---: | ---: |
| `demo-normal` | `/work` | 12 | 12 | 2 | 2 |
| `demo-normal` | `/bulk` | 4 | 4 | 1 | 1 |
| `demo-aggressive` | `/work` | 8 | 16 | 2 | 3 |
| `demo-aggressive` | `/bulk` | 2 | 4 | 1 | 1 |

Шлюз сначала тратит токен, затем пытается занять слот. Если слоты заняты, запрос ждёт в FIFO до 200 мс при наличии места. Токен остаётся списанным при отказе очереди, отмене или ошибке backend.

- `429` и `Retry-After` в целых секундах — частота превышена.
- `503` — очередь заполнена или истекло ожидание. Эти случаи различаются по `title` в Problem Details.
- `504` — backend не ответил полностью за 500 мс. Ошибка соединения даёт `502`.
- Тела запроса и ответа буферизуются с пределом 64 КиБ. На чтение тела запроса отведено до 2 секунд.

Режим по умолчанию — `custom`. Для сравнения запустите шлюз с `-- --Gate:Mode=standard` или `-- --Gate:Mode=off`. Во втором случае идентификация и ограничения HTTP-прокси продолжают действовать. Команды нагрузки и результаты есть в [демо](docs/demo.md) и [отчёте проверки](docs/verification.md).

## Проверка

```sh
dotnet test -c Release --no-build
dotnet format --verify-no-changes --no-restore
```

Сценарии ошибок перечислены в [docs/failure-scenarios.md](docs/failure-scenarios.md). Для установки CI скопируйте `FairGate/ci/request-limiter.yml` в `.github/workflows/request-limiter.yml` в корне репозитория. Шаблон рассчитан на расположение проекта в `FairGate/`.

## Границы

Квоты живут в памяти одного процесса и сбрасываются при перезапуске. Симулятор обслуживает до четырёх запросов одновременно на оба маршрута. Поэтому раздельные квоты защищают доступ к слотам шлюза, а задержка всё ещё зависит от общей ёмкости backend. Ключи демонстрационные, оба процесса слушают loopback по умолчанию. `/health` проверяет доступность процесса шлюза, а не backend.
