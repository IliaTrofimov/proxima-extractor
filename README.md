# Proxima Extractor
Скрипт для выгрузки данных из API на БД компании. Сделан мной для АО "Байер", к сожалению, договор с поставщиком данных был расторгнут, поэтому проект более не используется. Все ключи и важные параметры я стёр, без них запустить проект и получить данные не получится.

В скрипте используется многопоточность для быстрой загрузки данных (очень долго приходят ответы с сервера). Для связи с БД используется SqlAlchemy, запросы к API отсылаются с помощью Requests.
