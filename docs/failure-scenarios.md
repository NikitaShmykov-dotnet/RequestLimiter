# Сценарии отказа

Запустите шлюз и симулятор по [README](../README.md). В командах ниже для Windows PowerShell используйте `curl.exe`.

| Сбой | Как воспроизвести | Ожидаемый результат |
| --- | --- | --- |
| Подмена клиента | `curl -i -H "X-Client-Id: normal" http://127.0.0.1:5070/work` | `401`, квота клиента не меняется |
| Превышение частоты | Запустить [нагрузку](demo.md) в режиме `custom` | `429` с `Retry-After: 1` для части запросов; растёт `rateRejected` |
| Переполнение очереди | Запустить тест `Backend_concurrency_and_queue_are_bounded_and_timeouts_release_capacity` | При двух активных и двух ожидающих запросах следующий получает `503` с `title: Queue is full` |
| Истечение ожидания | Тот же тест с переводом часов на 200 мс | Два ожидающих запроса получают `503` с `title: Queue wait timed out` и до backend не доходят |
| Разрыв соединения | Запустить тест `Disconnect_reclaims_waiting_and_running_capacity` | Слот активного запроса освобождается, очередь продолжает работу; `inFlight` возвращается к нулю |
| Медленный backend | `curl -i -H "X-Api-Key: demo-normal" "http://127.0.0.1:5070/work?delayMs=1000"` | `504`, запрос к backend отменяется |
| Backend прислал заголовки и завис при передаче тела | Запустить тест `Backend_body_deadline_releases_capacity` | `504`, оба слота доступны следующей паре запросов |
| Backend прислал тело больше 64 КиБ | Запустить тест `Oversized_backend_body_releases_capacity` | `502`, оба слота доступны следующей паре запросов |
| Клиент отправил часть тела и остановился | Запустить тест `Slow_upload_times_out_without_contacting_backend` | `408`, backend не получает запрос, оба слота доступны следующей паре запросов |
| Backend выключен | Остановить симулятор и повторить запрос с ключом | `502`, слот освобождается |
| Слишком большое тело | Запустить тест `Oversized_body_never_reaches_backend` | `413` с `Content-Length` и при chunked передаче |

Команда для отдельного сценария:

```sh
dotnet test -c Release --filter FullyQualifiedName~Backend_concurrency_and_queue_are_bounded_and_timeouts_release_capacity
```

В HTTP-тестах два сервера Kestrel работают на динамических loopback-портах в одном тестовом процессе. Тестовый backend держит запрос до сигнала из теста. Тайм-ауты шлюза проверяются с `FakeTimeProvider`. Реальные тайм-ауты тестового клиента ограничивают зависание теста.
