# lamport-mutex

Зависимости:
python 3.5.2 + asyncio, pytest==3.0.5, pytest-asyncio==0.5.0
 
Сборка:
```bash
$ git clone https://github.com/AmatanHead/lamport-mutex.git
$ cd lamport-mutex
$ pip install -e .[dev]
```
 
Запуск тестов:
```bash
$ py.test tests/
```
 
Запуск стресс-теста:

- 10 процессов, 60 секунд.
- Логи складываются в директорию logs/, которая предварительно очищается.
- Flock лочит файл mtx.lock.
- После теста сразу запускается анализатор.

```bash
$ lm stress 10 60 --log-dir logs/ --clean-log-dir --lock-file mtx.lock --check
```
*NB:* файл `mtx.lock` должен быть создан до запуска команды. Иначе оно падает при попытке залочить несуществующий файл.
 
Запуск анализатора отельно:
```
$ lm log_check --log-dir logs/ --lock-file mtx.lock
```
*NB:* проверялка логов проверяет все файлы `*.log` в указанной директории.
Если в директории оказались логи от разных запусков, проверялка будет выдавать ошиьки.
Поэтому папку с логами нужно чистить.
 
Запуск в интерактивном режиме (`-V` for `Verbose`):
```bash
$ lm run 8001 8002 -VV  # левая половина tmux
$ lm run 8002 8001 -VV  # правая половина tmux
```

После запуска можно передавать в stdin команды `acquire`, `release`, `exit`.
