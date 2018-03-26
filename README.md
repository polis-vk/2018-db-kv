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
