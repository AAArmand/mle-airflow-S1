from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='7291353028:AAG4nHLo3nKEHmsxtS1iosi_GArbHkxM3O4',
                        chat_id='-4277793687')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4277793687',
        'text': message
    })


def send_telegram_failure_message(context):
	# ваш код здесь #
    hook = TelegramHook(telegram_conn_id='test',
                        token='7291353028:AAG4nHLo3nKEHmsxtS1iosi_GArbHkxM3O4',
                        chat_id='-4277793687')
    key = context['task_instance_key_str']
    run_id = context['run_id']
    
    message = f'Исполнение DAG c ключом {key} с id={run_id} не прошла :(' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4277793687',
        'text': message
    })
