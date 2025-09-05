# Итоговый проект

### Описание
Репозиторий предназначен для сдачи итогового проекта.

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de0-project-final` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/yandex-praktikum/de0-project-final`
3. Перейдите в директорию с проектом: 
	* `cd de0-project-final`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GitHub-аккаунте:
	* `git push origin main`

### Структура репозитория
Файлы в репозитории будут использоваться для проверки и обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре: так будет проще соотнести задания с решениями.

Внутри `src` расположены папки:
- `/src/dags` - вложите в эту папку код DAG, который поставляет данные из источника в хранилище. Назовите DAG `1_data_import.py`. Также разместите здесь DAG, который обновляет витрины данных. Назовите DAG `2_datamart_update.py`.
- `/src/sql` - сюда вложите SQL-запрос формирования таблиц в `STAGING`- и `DWH`-слоях, а также скрипт подготовки данных для итоговой витрины.
- `/src/py` - если источником вы выберете Kafka, то в этой папке разместите код запуска генерации и чтения данных в топик.


# Итоговый проект

### Описание
Репозиторий предназначен для сдачи итогового проекта.

### Docker образ репозитория скачать
docker pull cr.yandex/crp1r8pht0n0gl25aug1/de-final-prj:latest

### Docker образ запустить с локалки
docker run -d -p 8998:8998 -p 8280:8280 -p 15432:5432 --name=de-final-prj-local cr.yandex/crp1r8pht0n0gl25aug1/de-final-prj:latest

### Docker образ перезапуск с локалки чтоб видел ДАГи
docker stop de-final-prj-local && docker rm de-final-prj-local

docker run -d \
  -p 8998:8998 \
  -p 8280:8280 \
  -p 15432:5432 \
  --name de-final-prj-local \
  \
  -v /mnt/c/Users/user/de0-project-final/src/dags:/opt/airflow/dags \
  \
  -v /mnt/c/Users/user/de0-project-final/src/sql:/opt/airflow/sql \
  \
  -e PYTHONPATH="/opt/airflow/lib:/opt/airflow/dags" \
  \
  -e AIRFLOW__CORE__DAGS_FOLDER="/opt/airflow/dags" \
  \
  cr.yandex/crp1r8pht0n0gl25aug1/de-final-prj:latest

# войти в контейнер
docker exec -it de-final-prj-local bash

# внутри контейнера доустановить библиотеку
pip install --no-cache-dir "psycopg[binary]==3.1.18" "vertica-python==1.3.0"
pip install tqdm

# ВО для проекта
sudo apt-get update
sudo apt-get install -y python3-venv
python3 -m venv venv
source ./venv/bin/activate
python -m pip install --upgrade pip

if [ -f requirements.txt ]; then
  pip install -r requirements.txt
fi

pip freeze > requirements.txt

deactivate
rm -rf venv

#  Vertica логин и пароль для работы с Вертикой в Add Connection http://localhost:8280/airflow/connection/add:
VERTICA_CONNECTION
vertica.tgcloudenv.ru
stv202506166
TFZX59u7ucnjGcl
5433
{"connection_timeout": 30, "read_timeout": 60, "backup_server_node": ["backup_host:5433"], "ssl": false}

# Для доступа к БД создана учётная запись в Add Connection http://localhost:8280/airflow/connection/add:
"database": "db1",
"host" : "rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net",
"port" : 6432,
"username" : “student”,
"password" : "de_student_112022"
{"sslmode": "require", "connect_timeout": 10}

# ДАГ "0_data_init_table" загружает схемы и таблицы в Vertica из файлов 1_stg.sql и 2_dwh.sql в папке de0-project-final\src\sql:
Так как ограничены права на создание схем в Vertica, все таблицы распределены по имеющимся двум слоям.

# ДАГ "1_data_import" загружает данные из Postgres в Vertica:
БД из Postgres из public.currencies и загружаем в Vertica STV202506166__STAGING.currencies
    rate_dt   |currency_code_from|currency_code_to|currency_with_div|load_dt                |
    ----------+------------------+----------------+-----------------+-----------------------+
    2022-10-01|               410|             420|            0.930|2025-07-06 08:32:55.386|
    2022-10-01|               410|             430|            0.930|2025-07-06 08:32:55.386|
    2022-10-01|               410|             440|            1.070|2025-07-06 08:32:55.386|

БД из Postgres из public.transactions и загружаем в Vertica STV202506166__STAGING.transactions
    operation_id                        |account_number_from|account_number_to|currency_code|country|status     |transaction_type    |amount    |transaction_dt         |load_dt                |
    ------------------------------------+-------------------+-----------------+-------------+-------+-----------+--------------------+----------+-----------------------+-----------------------+
    00005a21-3599-4a4a-96d2-d6a62f41c3bf|                 -1|          1006757|          420|usa    |in_progress|sbp_incoming        |  21000.00|2022-10-20 10:04:05.000|2025-07-06 08:33:22.270|
    00005a21-3599-4a4a-96d2-d6a62f41c3bf|                 -1|          1006757|          420|usa    |chargeback |sbp_incoming        |  21000.00|2022-10-20 17:04:17.000|2025-07-06 08:33:22.270|
    00005a21-3599-4a4a-96d2-d6a62f41c3bf|            7127722|          4125072|          460|england|in_progress|sbp_incoming        |  21000.00|2022-10-20 10:04:05.000|2025-07-06 08:33:22.270|

# ДАГ "2_datamart_update" и  "2_datamart_update_2022-10-31" загружает данные из Vertica STV202506166__STAGING в Vertica STV202506166__DWH. "2_datamart_update_2022-10-31" - тест для загрузки таблиц для одной даты чтоб долго даг не грузился но проверить его работоспособность. утром когда впн летает, ресурсы не перегружены "2_datamart_update" отработал успешно и загрузил таблицы.

# STV202506166__DWH.dim_rates
    rate_dt   |rate_year|rate_month|rate_day|currency|rate_to_usd|load_dt                |
    ----------+---------+----------+--------+--------+-----------+-----------------------+
    2022-10-02|     2022|        10|       2|     430|   1.050000|2025-07-06 08:31:26.101|
    2022-10-02|     2022|        10|       2|     430|   0.925926|2025-07-06 08:31:26.101|
    2022-10-02|     2022|        10|       2|     440|   1.010000|2025-07-06 08:31:26.101|

# STV202506166__DWH.accounts
    account_number|country|load_dt                |
    --------------+-------+-----------------------+
                78|england|2025-07-06 08:38:10.040|
               147|russia |2025-07-06 08:38:10.040|
               168|usa    |2025-07-06 08:38:10.040|

# STV202506166__DWH.fact_transactions
    operation_id                        |account_number_from|account_number_to|currency_code|amount_src|amount_usd|trx_date  |trx_year|trx_month|trx_day|load_dt                |
    ------------------------------------+-------------------+-----------------+-------------+----------+----------+----------+--------+---------+-------+-----------------------+
    00014398-2762-430b-a79f-6c838a191e7e|             918366|           686514|          430| 150000.00| 144000.00|2022-10-05|    2022|       10|      5|2025-07-06 08:34:27.922|
    00014398-2762-430b-a79f-6c838a191e7e|             918366|           686514|          430| 150000.00| 156250.05|2022-10-05|    2022|       10|      5|2025-07-06 08:34:27.922|
    00078228-5404-4cf2-a8e2-f347b73f6703|            4732752|          6892719|          430|  25900.00|  24864.00|2022-10-05|    2022|       10|      5|2025-07-06 08:34:27.922|

# STV202506166__DWH.global_metrics
    date_update|currency_from|amount_total |cnt_transactions|avg_transactions_per_account|cnt_accounts_make_transactions|load_dt                |
    -----------+-------------+-------------+----------------+----------------------------+------------------------------+-----------------------+
    2022-10-31|          420| 819043678.00|            4842|                 185303.9995|                          4420|2025-07-05 22:34:23.631|
    2022-10-31|          430|6587850542.46|           39070|                 379200.5147|                         17373|2025-07-05 22:34:23.631|
    2022-10-31|          450| 976264249.94|            6806|                 313911.3344|                          3110|2025-07-05 22:34:23.631|