# 2018-db-kv
Курсовой проект 2018 года [курса](https://polis.mail.ru/curriculum/program/discipline/604/) "Использование баз данных" в [Технополис](https://polis.mail.ru).

## Этап 1. In-memory (deadline 2018-04-10)
### Fork
[Форкните проект](https://help.github.com/articles/fork-a-repo/), склонируйте и добавьте `upstream`:
```
$ git clone git@github.com:<username>/2018-db-kv.git
Cloning into '2018-db-kv'...
...
$ cd 2018-db-kv
$ git remote add upstream git@github.com:polis-mail-ru/2018-db-kv.git
$ git fetch upstream
From github.com:polis-mail-ru/2018-db-kv
 * [new branch]      master     -> upstream/master
```

### Make
Так можно запустить интерактивную консоль:
```
$ gradle run
```

А вот так -- тесты:
```
$ gradle test
```

### Develop
Откройте в IDE -- [IntelliJ IDEA Community Edition](https://www.jetbrains.com/idea/) нам будет достаточно.

В своём Java package `ru.mail.polis.<username>` реализуйте интерфейс [`KVDao`](src/main/java/ru/mail/polis/KVDao.java), используя одну из реализаций `java.util.Map`. 

Возвращайте свою реализацию интерфейса в [`KVServiceFactory`](src/main/java/ru/mail/polis/KVDaoFactory.java#L48).

Продолжайте запускать тесты и исправлять ошибки, не забывая [подтягивать новые тесты и фиксы из `upstream`](https://help.github.com/articles/syncing-a-fork/). Если заметите ошибку в `upstream`, заводите баг и присылайте pull request ;)

### Report
Когда всё будет готово, присылайте pull request со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!

## Этап 2. Persistence (deadline 2018-05-02)

На данном этапе необходимо реализовать персистентное хранение данных на диске. Папка, в которую нужно складывать файлы, передаётся в качестве параметра `KVDaoFactory.create()`, где конструируется Ваша реализация `KVDao`.

Как и раньше необходимо обеспечить прохождение существующих модульных тестов, а также желательно добавить свои тесты и включить их в pull request.

Предложения по реализации:
* Начать необходимо с хранения каждого значения в отдельном файле, но этого недостаточно, чтобы пройти этап 2.1
* Далее можно перейти на одно из [существующих Key-Value хранилищ](https://github.com/lmdbjava/benchmarks)
* Либо самостоятельно реализовать идею [LSM](https://en.wikipedia.org/wiki/Log-structured_merge-tree) как в [Cassandra](https://docs.datastax.com/en/cassandra/3.0/cassandra/dml/dmlHowDataWritten.html#dmlHowDataWritten__storing-data-on-disk-in-sstables) или [LevelDB](https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/), но без дополнительных индексов, сжатия и Bloom Filter

### Этап 2.1. Load test (deadline 2018-05-09)

Удалите аннотации `@Ignore` в наборе тестов `LoadTest` и обеспечьте прохождение тестов за разумное время (меньше 10 с).

Возможно, придётся пересмотреть подход к хранению данных.
