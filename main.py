import json
import os
import time
import asyncio
import threading
import random
import traceback
import sys
import requests
from kivy.app import App
from kivy.uix.widget import Widget
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.textinput import TextInput
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView
from kivy.uix.popup import Popup
from kivy.clock import Clock
from kivy.metrics import dp, sp
from kivy.graphics import Color, RoundedRectangle
from kivy.utils import platform
from kivy.core.window import Window
import ssl
import websockets
from websockets.exceptions import ConnectionClosed

# Исправление для Windows Proactor
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # Настройка кодировки вывода для Windows (осторожно, чтобы не сломать потоки Kivy)
    import io
    try:
        # Пробуем reconfigure, если доступно
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        # Если reconfigure не сработал, пробуем обернуть buffer, если он доступен
        try:
            if hasattr(sys.stdout, 'buffer'):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
            if hasattr(sys.stderr, 'buffer'):
                sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
        except Exception:
            pass  # Если ничего не вышло, оставляем как есть

# Конфигурация (можно вынести в config.json)
WS_URI = "wss://ws-api.oneme.ru/websocket"
TOKEN = "An_Sx6HQ9HDijaNsT_MidUdzBLdmqndAjNC_CO3Bn81E9iz0OJv3tRVhFdxD-wrLlnSmUud-YaOrInZZxMgP5Dnt0e0VO-bunpN0xf6geMcFNrfqhZ2R8h9QBLJtlSYIE5Ufw_ziphzT-ixlZW4CvZoazvzzlXxDAT7RddrkAIo-bn956KU_ywODX2tbno9KCY5ZKu_x9TkNahptMVQifqQmMJcGXlYACEFqq2D7NOQ8bYJTTtjizYO2FhUwwrc5zvdSA0Mu9XQBykH2juQapm4z6bdGFyRRTdNVpbI21i9gCp9-5EoiC8-KjFOG3KzxsL50jaZ9Ix2nC3TTU3hFnntr-_MH39QJOdCN0I6fGjMdJees2W-LGyMCwQ8bniOAj3vN5_U-LQaCPEauU2eaOVB5WgGgbQ_otSyPz4BEM_MVf5diDMFkXqq0JIfFYaQQXtF8_3j9qyMvgLRBj6Twzsnxwvh5fePFYEjjW1tTPli_u8el9_t06C10E4QWWWlG18tvS-M-PF4r538fm0M8cdTGsAntYw1XmVRLmjH6bpIz7y8SEB0cWkcuK_EWgTkagv5JzBWsS1WWX89jTqxSSj8h4vAgAK2HvV-WPC-TPaZavk8mLruMruYFrPynFnf7AwWh7z96EMhzLNdCo6eSw0cdNFXoCEhsqQqOB4Kmu_NuNM1zMROtNBQsOiIHpxWpLTxSWjs"
DEVICE_ID = "04e30b88-9c09-40f2-9ebd-9540fd60a4cd"
MY_ID = "62093986"

# Заголовки для имитации веб-клиента
ORIGIN = "https://web.max.ru"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"

# Ограничения кэша
MAX_CACHED_CHATS = 50  # максимальное количество чатов, хранимых в кэше
MAX_MESSAGES_PER_CHAT = 50  # максимальное количество сообщений на чат

# Загрузка конфигурации из файла config.json
CONFIG_FILE = "config.json"
if os.path.exists(CONFIG_FILE):
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
        TOKEN = config.get('token', TOKEN)
        DEVICE_ID = config.get('device_id', DEVICE_ID)
        MY_ID = config.get('my_id', MY_ID)
        print(f"[CONF2IG] Загружены параметры из {CONFIG_FILE}")
    except Exception as e:
        print(f"[CONFIG] Ошибка загрузки конфигурации: {e}")
else:
    print(f"[CONFIG] Файл {CONFIG_FILE} не найден, используются значения по умолчанию")

# Пути к файлам
if platform == 'android':
    app_path = '/data/user/0/org.test.maxmessenger/files/app'
    CACHE_FILE = os.path.join(app_path, "maxa_cache.json")
    PENDING_FILE = os.path.join(app_path, "maxa_pending.json")
    HISTORY_REQUEST_FILE = os.path.join(app_path, "maxa_history_request.json")
else:
    CACHE_FILE = "maxa_cache.json"
    PENDING_FILE = "maxa_pending.json"
    HISTORY_REQUEST_FILE = "maxa_history_request.json"

# HTTP API константы
HTTP_API_BASE = "https://api.oneme.ru"
HTTP_API_PLATFORM = "https://platform-api.max.ru"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"

class SimpleMessageBubble(Widget):
    """Упрощённый пузырь сообщения (без времени)"""
    def __init__(self, text, side='left', timestamp=None, **kwargs):
        super().__init__(**kwargs)
        self.size_hint = (None, None)
        bg = (0.07, 0.45, 0.8, 1) if side == 'right' else (0.2, 0.2, 0.25, 1)
        # Основной текст
        self.lbl = Label(text=text, size_hint=(None, None), halign='left', valign='top',
                         font_size=sp(14), color=(1,1,1,1), text_size=(dp(220), None))
        self.lbl.bind(texture_size=self._update_size)
        with self.canvas.before:
            Color(*bg)
            self.rect = RoundedRectangle(radius=[dp(12)])
        self.bind(pos=self._update_rect, size=self._update_rect)
        self.add_widget(self.lbl)
        # Внутренние отступы
        self.padding = [dp(12), dp(8)]  # горизонтальный, вертикальный

    def _update_size(self, instance, size):
        # Ограничиваем ширину текста
        self.lbl.width = min(size[0], dp(220))
        self.lbl.text_size = (self.lbl.width, None)
        # Используем высоту текстуры текста
        text_height = size[1]
        self.lbl.height = text_height
        # Учитываем внутренние отступы (padding)
        pad_h = self.padding[0] * 2  # горизонтальные padding слева и справа
        pad_v = self.padding[1] * 2  # вертикальные padding сверху и снизу
        total_height = text_height + pad_v
        total_width = self.lbl.width + pad_h
        self.size = (total_width, total_height)
        # Позиционируем текст с учётом padding
        self.lbl.pos = (self.x + self.padding[0], self.y + self.padding[1])

    def _update_rect(self, *args):
        self.rect.pos, self.rect.size = self.pos, self.size
        # Также обновляем позицию текста при изменении позиции пузыря
        self.lbl.pos = (self.x + self.padding[0], self.y + self.padding[1])

class SimpleMaxApp(App):
    """Упрощённое приложение МАХ"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cache = {"names": {}, "history": {}}  # history теперь используется только как временный кэш в памяти
        self.current_chat_id = None
        self.network_state = "offline"  # offline, connecting, online, error
        self.network_thread = None
        self.status_label = None
        self.messages_box = None
        self.input_field = None
        self.chat_selector = None
        self.chat_list = {}  # id -> name
        self.app_paused = False
        self.reconnect_attempt = 0
        self.seq_counter = 0
        self.reconnect_delay = 1  # начальная задержка в секундах
        self.sync_marker = 0  # marker из ответа синхронизации (opcode 53)
        self.session_ready = False  # флаг, что сессия готова для запроса истории
        self.session_online = False  # флаг, что сессия перешла в состояние ONLINE (после ответа на приветствие)
        self.sync_sent = False  # флаг, что синхронизация уже отправлена
        self.chat_history_cache = {}  # кэш истории чатов в памяти: chat_id -> список сообщений
        self.history_request_timestamps = {}  # время последнего запроса истории для каждого чата
        self.history_request_cooldown = 2  # минимальная задержка между запросами истории (секунды) - уменьшено для быстрого переключения на HTTP

    def next_seq(self):
        """Возвращает следующий порядковый номер для WebSocket-сообщений"""
        self.seq_counter += 1
        return self.seq_counter

    def on_pause(self):
        """При сворачивании приложения оставляем его работать в фоне"""
        self.app_paused = True
        print("[APP] Приложение свёрнуто (пауза)")
        return True

    def on_resume(self):
        """При возобновлении обновляем интерфейс"""
        self.app_paused = False
        self.reconnect_attempt = 0
        self.reconnect_delay = 1
        print("[APP] Приложение возобновлено")

    def on_request_close(self, *args):
        """При попытке закрытия сворачиваем приложение (только для Android)"""
        from kivy.utils import platform
        if platform == 'android':
            # Сворачиваем вместо закрытия
            from jnius import autoclass
            PythonActivity = autoclass('org.kivy.android.PythonActivity')
            activity = PythonActivity.mActivity
            activity.moveTaskToBack(True)
            return True
        return False

    def build(self):
        # Настройка поведения клавиатуры на Android
        from kivy.core.window import Window
        Window.softinput_mode = 'pan'  # сдвигает содержимое, чтобы поле ввода было видно
        
        # Основной макет
        root = BoxLayout(orientation='vertical')

        # Верхняя панель с индикатором сети
        top_panel = BoxLayout(size_hint_y=None, height=dp(40), padding=dp(5))
        self.status_label = Label(text="OFFLINE", color=(1,0,0,1), bold=True, halign='left')
        title_label = Label(text="MAX Simple", bold=True, size_hint_x=0.7)
        top_panel.add_widget(title_label)
        top_panel.add_widget(self.status_label)

        # Область сообщений
        messages_scroll = ScrollView(do_scroll_x=False)
        self.messages_box = BoxLayout(orientation='vertical', size_hint_y=None, spacing=dp(10), padding=dp(10))
        self.messages_box.bind(minimum_height=self.messages_box.setter('height'))
        messages_scroll.add_widget(self.messages_box)

        # Панель ввода
        input_panel = BoxLayout(size_hint_y=None, height=dp(50), spacing=dp(5), padding=dp(5))
        self.input_field = TextInput(hint_text="Введите сообщение...", multiline=False)
        self.input_field.bind(on_text_validate=self.send_message)
        send_button = Button(text="Отправить", size_hint_x=0.3)
        send_button.bind(on_press=self.send_message)
        input_panel.add_widget(self.input_field)
        input_panel.add_widget(send_button)

        # Список чатов (упрощённый - просто кнопка выбора)
        chat_panel = BoxLayout(size_hint_y=None, height=dp(40), spacing=dp(5), padding=dp(5))
        chat_label = Label(text="Чаты:", size_hint_x=0.2)
        self.chat_selector = Button(text="Выберите чат", size_hint_x=0.6)
        self.chat_selector.bind(on_press=self.show_chat_list)
        refresh_button = Button(text="Ист.", size_hint_x=0.2)
        refresh_button.bind(on_press=self.request_history)
        chat_panel.add_widget(chat_label)
        chat_panel.add_widget(self.chat_selector)
        chat_panel.add_widget(refresh_button)

        root.add_widget(top_panel)
        root.add_widget(chat_panel)
        root.add_widget(messages_scroll)
        root.add_widget(input_panel)

        # Запуск сетевого клиента
        Clock.schedule_once(lambda dt: self.start_network_thread(), 1)
        Clock.schedule_interval(self.update_network_status, 2)
        # Загрузить чаты из кэша
        Clock.schedule_once(lambda dt: self.load_chats_from_cache(), 0.5)

        return root

    def load_chats_from_cache(self):
        """Загрузить список чатов из кэша и историю в память (для обратной совместимости)"""
        if not os.path.exists(CACHE_FILE):
            return
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            self.cache = cache
            self.chat_list = cache.get('names', {})
            
            # Загружаем историю из файла в кэш памяти (для обратной совместимости)
            file_history = cache.get('history', {})
            for cid_str, messages in file_history.items():
                if cid_str not in self.chat_history_cache:
                    self.chat_history_cache[cid_str] = []
                
                # Добавляем сообщения, избегая дубликатов
                existing_ids = {msg.get('id') for msg in self.chat_history_cache[cid_str] if msg.get('id')}
                for msg in messages:
                    msg_id = msg.get('id')
                    if msg_id and msg_id in existing_ids:
                        continue
                    
                    # Преобразуем формат сообщения
                    msg_obj = {
                        "text": msg.get("text", ""),
                        "side": msg.get("side", "left"),
                        "time": msg.get("time", time.time())
                    }
                    if msg_id:
                        msg_obj["id"] = msg_id
                    if "reactions" in msg:
                        msg_obj["reactions"] = msg["reactions"]
                    
                    self.chat_history_cache[cid_str].append(msg_obj)
                
                # Ограничиваем количество сообщений
                if len(self.chat_history_cache[cid_str]) > MAX_MESSAGES_PER_CHAT:
                    self.chat_history_cache[cid_str] = self.chat_history_cache[cid_str][-MAX_MESSAGES_PER_CHAT:]
                
                print(f"[CACHE] Загружено {len(messages)} сообщений для чата {cid_str} из файла")
            
            # Если есть история, но нет имён, создаём имена
            for cid in cache.get('history', {}).keys():
                if cid not in self.chat_list:
                    self.chat_list[cid] = f"Чат {cid}"
        except Exception as e:
            print(f"Ошибка загрузки кэша: {e}")

    def show_chat_list(self, instance):
        """Показать всплывающее окно со списком чатов"""
        self.load_chats_from_cache()
        if not self.chat_list:
            self.chat_selector.text = "Нет чатов"
            return
        
        content = BoxLayout(orientation='vertical', spacing=dp(5), padding=dp(10))
        scroll = ScrollView()
        list_layout = BoxLayout(orientation='vertical', size_hint_y=None, spacing=dp(2))
        list_layout.bind(minimum_height=list_layout.setter('height'))
        
        # Получить историю для определения времени последнего сообщения
        history = self.cache.get('history', {})
        # Создать список чатов с временем последнего сообщения
        chat_items = []
        for cid, name in self.chat_list.items():
            last_time = 0
            if cid in history and history[cid]:
                last_msg = history[cid][-1]
                last_time = last_msg.get('time', 0)
            chat_items.append((cid, name, last_time))
        # Сортировка по времени убывания (самые свежие сверху)
        chat_items.sort(key=lambda x: x[2], reverse=True)
        
        for cid, name, _ in chat_items:
            row = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(40), spacing=dp(2))
            chat_btn = Button(text=name, size_hint_x=0.8,
                              background_color=(0.2, 0.2, 0.3, 1))
            chat_btn.bind(on_release=lambda btn, cid=cid: (self.switch_chat(cid), popup.dismiss()))
            edit_btn = Button(text="✏️", size_hint_x=0.2, font_size=sp(20))
            edit_btn.bind(on_release=lambda btn, cid=cid: self.rename_chat_dialog(cid, popup))
            row.add_widget(chat_btn)
            row.add_widget(edit_btn)
            list_layout.add_widget(row)
        
        scroll.add_widget(list_layout)
        content.add_widget(scroll)
        
        close_btn = Button(text="Закрыть", size_hint_y=None, height=dp(40))
        popup = Popup(title="Выберите чат", content=content, size_hint=(0.8, 0.8))
        close_btn.bind(on_press=popup.dismiss)
        content.add_widget(close_btn)
        popup.open()

    def rename_chat_dialog(self, chat_id, parent_popup):
        """Открыть диалог переименования чата"""
        parent_popup.dismiss()  # закрыть список чатов
        content = BoxLayout(orientation='vertical', spacing=dp(10), padding=dp(20))
        input_field = TextInput(text=self.chat_list.get(str(chat_id), f"Чат {chat_id}"),
                                multiline=False, size_hint_y=None, height=dp(40))
        content.add_widget(input_field)
        
        btn_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(40), spacing=dp(5))
        save_btn = Button(text="Сохранить")
        cancel_btn = Button(text="Отмена")
        btn_layout.add_widget(save_btn)
        btn_layout.add_widget(cancel_btn)
        content.add_widget(btn_layout)
        
        popup = Popup(title=f"Переименовать чат {chat_id}", content=content,
                      size_hint=(0.8, 0.4))
        
        def save_name(instance):
            new_name = input_field.text.strip()
            if new_name:
                self.chat_list[str(chat_id)] = new_name
                # Сохранить в кэш
                try:
                    with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                        cache = json.load(f)
                except:
                    cache = {"names": {}, "history": {}}
                cache["names"][str(chat_id)] = new_name
                with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                    json.dump(cache, f, ensure_ascii=False, indent=2)
                print(f"[UI] Чат {chat_id} переименован в '{new_name}'")
                popup.dismiss()
                # Обновить список чатов
                self.show_chat_list(None)
            else:
                input_field.hint_text = "Введите имя"
        
        def cancel(instance):
            popup.dismiss()
            self.show_chat_list(None)
        
        save_btn.bind(on_press=save_name)
        cancel_btn.bind(on_press=cancel)
        popup.open()

    def switch_chat(self, chat_id):
        """Переключиться на выбранный чат"""
        chat_id_str = str(chat_id)
        self.current_chat_id = chat_id_str
        name = self.chat_list.get(chat_id_str, f"Чат {chat_id}")
        self.chat_selector.text = name
        print(f"[UI] Выбран чат: {chat_id} ({name})")
        # Очистить текущие сообщения
        self.messages_box.clear_widgets()
        
        # Проверить, есть ли история в кэше памяти
        messages_count = 0
        if chat_id_str in self.chat_history_cache and self.chat_history_cache[chat_id_str]:
            # Загрузить историю из кэша памяти
            messages = self.chat_history_cache[chat_id_str]
            messages_count = len(messages)
            for msg in messages:
                self.add_message_to_ui(msg['text'], msg['side'], timestamp=msg.get('time'), reactions=msg.get('reactions'))
            print(f"[UI] Загружено {messages_count} сообщений из кэша памяти")
        
        # Если в кэше меньше 10 сообщений, запросить дополнительную историю с сервера
        if messages_count < 10:
            print(f"[UI] В кэше только {messages_count} сообщений, запрашиваем дополнительную историю с сервера")
            self.request_chat_history_from_server(chat_id_str)
        else:
            print(f"[UI] В кэше достаточно сообщений ({messages_count}), дополнительный запрос не требуется")
        
        # Прокрутить вниз
        Clock.schedule_once(lambda dt: setattr(self.messages_box.parent, 'scroll_y', 0), 0.1)

    def request_chat_history_from_server(self, chat_id):
        """Запросить историю чата с сервера (использует WebSocket opcode 49)"""
        import time
        import json
        current_time = time.time()
        chat_id_str = str(chat_id)
        
        # Проверка cooldown (используем тот же таймер, что и для HTTP, но не обновляем его)
        last_request = self.history_request_timestamps.get(chat_id_str, 0)
        if current_time - last_request < self.history_request_cooldown:
            print(f"[UI] WebSocket запрос истории для чата {chat_id} игнорируется (cooldown)")
            return
        
        # Не обновляем timestamp, чтобы не блокировать последующие HTTP запросы
        # self.history_request_timestamps[chat_id_str] = current_time
        
        # Создаём файл запроса для сетевого потока
        try:
            with open(HISTORY_REQUEST_FILE, 'w', encoding='utf-8') as f:
                json.dump({"chatId": chat_id_str, "type": "opcode49"}, f)
            print(f"[UI] Создан запрос истории через WebSocket: {HISTORY_REQUEST_FILE}, chatId={chat_id_str}")
        except Exception as e:
            print(f"[UI] Ошибка создания файла запроса истории: {e}")
        
    def request_chat_history_via_http(self, chat_id):
        """Запросить историю чата через HTTP API (запасной вариант) с пагинацией"""
        import time
        current_time = time.time()
        chat_id_str = str(chat_id)
        
        # Проверка cooldown
        last_request = self.history_request_timestamps.get(chat_id_str, 0)
        if current_time - last_request < self.history_request_cooldown:
            print(f"[UI] HTTP запрос истории для чата {chat_id} игнорируется (cooldown)")
            return
        
        # Обновляем время последнего запроса
        self.history_request_timestamps[chat_id_str] = current_time
        
        # Формируем URL и заголовки
        # Пробуем несколько возможных endpoint'ов на основе информации о API MAX
        candidates = [
            # GET /messages с query параметрами (предположительно)
            (HTTP_API_BASE, "/messages", "GET"),
            (HTTP_API_PLATFORM, "/messages", "GET"),
            # POST с JSON payload (старый вариант)
            (HTTP_API_BASE, f"/{chat_id_str}/messages", "POST"),
            (HTTP_API_PLATFORM, f"/v1/chat/{chat_id_str}/messages", "POST"),
        ]
        
        url = None
        method = "GET"
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json"
        }
        
        # Пробуем каждый кандидат
        for base, path, candidate_method in candidates:
            candidate_url = base.rstrip('/') + path
            print(f"[UI] Пробуем HTTP API URL: {candidate_url}, метод: {candidate_method}")
            try:
                # Пробный запрос с минимальными параметрами
                params = {"chat_id": chat_id_str, "count": 1, "offset": 0}
                if candidate_method == "GET":
                    response = requests.get(candidate_url, params=params, headers=headers, timeout=10)
                else:
                    # POST с JSON payload
                    response = requests.post(candidate_url, json=params, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, dict) and "messages" in data:
                        url = candidate_url
                        method = candidate_method
                        print(f"[UI] Найден рабочий endpoint: {url}, метод: {method}")
                        break
                    else:
                        print(f"[UI] Endpoint вернул неожиданный формат: {data}")
                else:
                    print(f"[UI] Endpoint вернул статус {response.status_code}")
            except Exception as e:
                print(f"[UI] Ошибка при проверке endpoint {candidate_url}: {e}")
        
        if not url:
            print(f"[UI] Не удалось найти рабочий HTTP endpoint для истории чата {chat_id}")
            return
        
        print(f"[UI] Используем HTTP API URL: {url}, метод: {method}")
        
        # Параметры пагинации
        count_per_request = 100  # максимальное количество сообщений за один запрос
        max_messages = 500       # общий лимит сообщений для загрузки
        offset = 0
        all_messages = []
        retry_count = 0
        max_retries = 3
        
        while offset < max_messages and retry_count < max_retries:
            # Формируем параметры запроса
            params = {
                "count": count_per_request,
                "offset": offset,
                "direction": "backward"
            }
            # Для GET добавляем chat_id, если его нет в URL
            if method == "GET" and chat_id_str not in url:
                params["chat_id"] = chat_id_str
            
            print(f"[UI] Отправка HTTP запроса истории для чата {chat_id} (offset={offset}, count={count_per_request}) на {url}")
            
            try:
                if method == "GET":
                    response = requests.get(url, params=params, headers=headers, timeout=15)
                else:
                    # POST с JSON payload
                    response = requests.post(url, json=params, headers=headers, timeout=15)
                
                # Обработка rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    print(f"[UI] Rate limit превышен, ждём {retry_after} секунд")
                    time.sleep(retry_after)
                    retry_count += 1
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                # Обработка ответа
                if isinstance(data, dict) and "messages" in data:
                    messages_data = data["messages"]
                    if not messages_data:
                        print(f"[UI] Достигнут конец истории (получено 0 сообщений)")
                        break
                    
                    print(f"[UI] Получено {len(messages_data)} сообщений через HTTP API (offset={offset})")
                    all_messages.extend(messages_data)
                    
                    # Если получено меньше, чем запрошено, значит история закончилась
                    if len(messages_data) < count_per_request:
                        break
                    
                    offset += len(messages_data)
                    retry_count = 0  # сброс счётчика повторных попыток
                else:
                    print(f"[UI] Неожиданный формат ответа HTTP API: {data}")
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"[UI] Ошибка HTTP запроса истории: {e}")
                break
            except Exception as e:
                print(f"[UI] Неожиданная ошибка при обработке HTTP ответа: {e}")
                break
        
        if not all_messages:
            print(f"[UI] Не удалось получить историю чата {chat_id} через HTTP API")
            return
        
        print(f"[UI] Всего получено {len(all_messages)} сообщений для чата {chat_id}")
        
        # Преобразуем в формат приложения
        formatted_messages = []
        for msg in all_messages:
            # Определяем сторону сообщения
            sender_id = msg.get("senderId")
            side = 'right' if str(sender_id) == str(MY_ID) else 'left'
            text = msg.get("text", "")
            timestamp = msg.get("timestamp")
            message_id = msg.get("id")
            reactions = msg.get("reactions")
            
            formatted_messages.append({
                'text': text,
                'side': side,
                'time': timestamp,
                'id': message_id,
                'reactions': reactions
            })
        
        # Сохраняем в кэш памяти
        if chat_id_str not in self.chat_history_cache:
            self.chat_history_cache[chat_id_str] = []
        # Добавляем новые сообщения, избегая дубликатов
        existing_ids = {m.get('id') for m in self.chat_history_cache[chat_id_str]}
        for msg in formatted_messages:
            if msg.get('id') not in existing_ids:
                self.chat_history_cache[chat_id_str].append(msg)
        
        # Ограничиваем размер кэша
        if len(self.chat_history_cache[chat_id_str]) > MAX_MESSAGES_PER_CHAT:
            self.chat_history_cache[chat_id_str] = self.chat_history_cache[chat_id_str][-MAX_MESSAGES_PER_CHAT:]
        
        # Сортируем по времени (старые -> новые)
        self.chat_history_cache[chat_id_str].sort(key=lambda x: x.get('time', 0))
        
        # Обновляем UI, если это текущий чат
        if chat_id_str == self.current_chat_id:
            self.reload_chat_history(chat_id_str)
        
        print(f"[UI] История чата {chat_id} обновлена через HTTP API, всего {len(self.chat_history_cache[chat_id_str])} сообщений в кэше")
    
    def reload_chat_history(self, chat_id):
        """Перезагрузить историю чата из кэша памяти и обновить UI"""
        chat_id_str = str(chat_id)
        if chat_id_str != self.current_chat_id:
            return
        self.messages_box.clear_widgets()
        
        # Загрузить историю из кэша памяти
        if chat_id_str in self.chat_history_cache:
            messages = self.chat_history_cache[chat_id_str]
            for msg in messages:
                self.add_message_to_ui(msg['text'], msg['side'], timestamp=msg.get('time'), reactions=msg.get('reactions'))
            print(f"[UI] Перезагружено {len(messages)} сообщений из кэша памяти")
        else:
            # Если нет в кэше, запросить с сервера
            self.request_chat_history_from_server(chat_id_str)
        
        Clock.schedule_once(lambda dt: setattr(self.messages_box.parent, 'scroll_y', 0), 0.1)

    def update_network_status(self, dt):
        """Обновить индикатор сети на основе network_state"""
        state = self.network_state
        if state == "online":
            self.status_label.text = "ONLINE"
            self.status_label.color = (0.2, 1, 0.2, 1)
        elif state == "connecting":
            self.status_label.text = "CONNECTING..."
            self.status_label.color = (1, 0.8, 0, 1)
        elif state == "error":
            self.status_label.text = "ERROR"
            self.status_label.color = (1, 0, 0, 1)
        else:  # offline
            self.status_label.text = "OFFLINE"
            self.status_label.color = (1, 0, 0, 1)

    def send_message(self, *args):
        """Отправить сообщение"""
        text = self.input_field.text.strip()
        if not text or not self.current_chat_id:
            print("[UI] Нельзя отправить: нет текста или выбранного чата")
            return
        # Сохраняем в pending файл
        pending_data = {"chatId": self.current_chat_id, "text": text}
        try:
            with open(PENDING_FILE, 'w', encoding='utf-8') as f:
                json.dump(pending_data, f)
            print(f"[UI] Создан pending файл: {PENDING_FILE}, chatId={self.current_chat_id}, text={text[:20]}...")
            # Локально добавляем сообщение
            self.add_message_to_ui(text, 'right', timestamp=time.time())
            self.input_field.text = ""
        except Exception as e:
            print(f"Ошибка отправки: {e}")

    def request_history(self, *args):
        """Запросить историю текущего чата через Opcode 7 (запрос истории) или HTTP API"""
        if not self.current_chat_id:
            print("[UI] Не выбран чат для запроса истории")
            return
        if self.sync_sent:
            print("[UI] Синхронизация уже выполнена, используем HTTP API для получения полной истории.")
            self.request_chat_history_via_http(self.current_chat_id)
            return
        try:
            with open(HISTORY_REQUEST_FILE, 'w', encoding='utf-8') as f:
                json.dump({"chatId": self.current_chat_id}, f)
            print(f"[UI] Создан запрос истории: {HISTORY_REQUEST_FILE}, chatId={self.current_chat_id}")
        except Exception as e:
            print(f"Ошибка создания запроса истории: {e}")

    def add_message_to_ui(self, text, side, timestamp=None, reactions=None):
        """Добавить сообщение в UI"""
        bubble = SimpleMessageBubble(text=text, side=side, timestamp=timestamp)
        # Создаём метку времени
        if timestamp is not None:
            from time import strftime, localtime
            time_str = strftime('%H:%M', localtime(timestamp))
        else:
            time_str = ''
        time_label = Label(text=time_str, size_hint=(None, None), font_size=sp(10),
                           color=(0.9,0.9,0.9,0.8), halign='right', valign='middle')
        time_label.bind(size=time_label.setter('text_size'))
        # Высота времени
        time_height = dp(14) if time_str else 0
        time_label.size_hint_y = None
        time_label.height = time_height
        
        # Горизонтальный контейнер для пузыря и времени
        holder = BoxLayout(orientation='horizontal', size_hint_y=None,
                           spacing=dp(5))
        if side == 'right':
            # Справа: пустое пространство слева, затем пузырь, затем время
            holder.add_widget(Label(size_hint_x=1))
            holder.add_widget(bubble)
            holder.add_widget(time_label)
        else:
            # Слева: пузырь, затем время, затем пустое пространство справа
            holder.add_widget(bubble)
            holder.add_widget(time_label)
            holder.add_widget(Label(size_hint_x=1))
        
        # Динамическое обновление высоты контейнера при изменении высоты пузыря
        def update_holder_height(instance, value):
            holder.height = max(bubble.height, time_height)
        bubble.bind(height=update_holder_height)
        # Установить начальную высоту
        holder.height = max(bubble.height, time_height)
        
        # Контейнер для всего сообщения (вертикальный)
        message_container = BoxLayout(orientation='vertical', size_hint_y=None, spacing=dp(2))
        message_container.add_widget(holder)
        
        # Добавляем реакции, если есть
        if reactions:
            # reactions может быть списком строк типа ["🤣 1", "💯 2"]
            reactions_text = "  ".join(reactions)
            reactions_label = Label(text=reactions_text, size_hint_y=None, font_size=sp(11),
                                    color=(0.7,0.7,0.7,1), halign='right' if side == 'right' else 'left')
            reactions_label.bind(size=reactions_label.setter('text_size'))
            reactions_label.height = dp(16)
            message_container.add_widget(reactions_label)
            # Увеличиваем высоту контейнера
            message_container.height = holder.height + reactions_label.height
        
        self.messages_box.add_widget(message_container)
        # Прокрутка вниз
        Clock.schedule_once(lambda dt: setattr(self.messages_box.parent, 'scroll_y', 0), 0.1)

    def start_network_thread(self):
        """Запустить сетевой клиент в отдельном потоке"""
        def run():
            asyncio.run(self.network_worker())
        self.network_thread = threading.Thread(target=run, daemon=True)
        self.network_thread.start()

    async def request_chat_history(self, ws, chat_id):
        """Запросить историю сообщений чата (opcode 7)"""
        try:
            seq = self.next_seq()
            payload = {
                "chatId": int(chat_id),
                "limit": 20,
                "offset": 0,
                "marker": self.sync_marker,  # используем marker из синхронизации
                "direction": "backward",
                "type": "ALL"
            }
            await ws.send(json.dumps({
                "ver": 11, "cmd": 0, "seq": seq, "opcode": 7,
                "payload": payload
            }))
            print(f"[NETWORK] Запрошена история чата {chat_id} (opcode 7) с marker={self.sync_marker}, seq={seq}")
        except Exception as e:
            print(f"[NETWORK] Ошибка запроса истории: {e}")

    async def periodic_updates(self, ws):
        """Периодическое обновление чатов и истории"""
        import time
        while True:
            try:
                await asyncio.sleep(120)  # каждые 120 секунд (2 минуты) вместо 60
                # Обновляем список чатов (opcode 19)
                seq = self.next_seq()
                await ws.send(json.dumps({
                    "ver": 11, "cmd": 0, "seq": seq,
                    "opcode": 19,
                    "payload": {"token": TOKEN, "chatsCount": 40, "interactive": True}
                }))
                print(f"[NETWORK] Периодический запрос обновления чатов, seq={seq}")
                
                # Если есть текущий чат, запросить его историю с учётом cooldown
                if self.current_chat_id:
                    current_time = time.time()
                    last_request = self.history_request_timestamps.get(self.current_chat_id, 0)
                    # Запрашиваем историю только если прошло больше 30 секунд с последнего запроса
                    if current_time - last_request > 30:
                        await self.request_chat_history(ws, self.current_chat_id)
                    else:
                        print(f"[NETWORK] Пропускаем запрос истории для чата {self.current_chat_id} (cooldown)")
            except Exception as e:
                print(f"[NETWORK] Ошибка периодического обновления: {e}")
                break

    async def network_worker(self):
        """Фоновая задача сетевого клиента"""
        # Инициализация файлов
        if not os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump({"names": {}, "history": {}}, f)

        while True:
            try:
                self.network_state = "connecting"
                # Создаём SSL контекст с отключённой проверкой сертификата (для Android)
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                async with websockets.connect(WS_URI, ping_interval=30, ping_timeout=15,
                                              origin=ORIGIN, ssl=ssl_context) as ws:
                    self.network_state = "online"
                    self.reconnect_attempt = 0  # сброс счётчика переподключений
                    print("[NETWORK] WebSocket подключён")
                    # Сброс флагов сессии
                    self.session_online = False
                    self.session_ready = False
                    self.sync_marker = 0

                    # Запуск периодических обновлений
                    update_task = asyncio.create_task(self.periodic_updates(ws))

                    # Отправляем приветствие от клиента (opcode 1 cmd 0)
                    await ws.send(json.dumps({
                        "ver": 11, "cmd": 0, "seq": self.next_seq(), "opcode": 1,
                        "payload": {"interactive": True}
                    }))
                    print("[NETWORK] Отправлено приветствие от клиента")

                    # Ждём подтверждения от сервера (opcode 1 cmd 1)
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        data = json.loads(raw)
                        op = data.get('opcode')
                        cmd = data.get('cmd')
                        if op == 1 and cmd == 1:
                            self.session_online = True
                            print("[NETWORK] Подтверждение приветствия получено, сессия ONLINE")
                        else:
                            print(f"[NETWORK] Неожиданный ответ на приветствие: op={op}, cmd={cmd}")
                    except asyncio.TimeoutError:
                        print("[NETWORK] Таймаут подтверждения приветствия, продолжаем без него")

                    # Логин с токеном
                    login_msg = {
                        "ver": 11, "cmd": 0, "seq": self.next_seq(), "opcode": 6,
                        "payload": {
                            "userAgent": {"deviceType": "WEB", "osVersion": "Android"},
                            "deviceId": DEVICE_ID,
                            "token": TOKEN
                        }
                    }
                    await ws.send(json.dumps(login_msg))
                    print("[NETWORK] Отправлен логин с токеном")

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            data = json.loads(raw)
                            op = data.get('opcode')
                            cmd = data.get('cmd')
                            payload = data.get('payload')

                            # Обработка ошибок от сервера (cmd == 3)
                            if cmd == 3:
                                error_msg = payload.get('message', 'Неизвестная ошибка') if payload else 'Нет деталей'
                                print(f"[NETWORK] Ошибка от сервера (op={op}): {error_msg}")
                                # Не разрываем соединение, продолжаем обработку
                                continue

                            # Обновляем время файла кэша как индикатор активности
                            os.utime(CACHE_FILE, None)

                            if op == 6 and cmd == 1:
                                # Логин успешен, запрашиваем чаты
                                print("[NETWORK] Логин успешен, запрашиваю чаты")
                                seq_chats = self.next_seq()
                                await ws.send(json.dumps({
                                    "ver": 11, "cmd": 0, "seq": seq_chats, "opcode": 19,
                                    "payload": {"token": TOKEN, "chatsCount": 40, "interactive": True}
                                }))
                                # Отложенная синхронизация через 5 секунд, чтобы дать возможность запросить историю до неё
                                if self.session_online and not self.sync_sent:
                                    async def delayed_sync():
                                        await asyncio.sleep(5.0)
                                        if not self.sync_sent:
                                            seq_sync = self.next_seq()
                                            await ws.send(json.dumps({
                                                "ver": 11, "cmd": 0, "seq": seq_sync, "opcode": 53,
                                                "payload": {"marker": 0}
                                            }))
                                            self.sync_sent = True
                                            print(f"[NETWORK] Отправлен отложенный запрос синхронизации (opcode 53), seq={seq_sync}")
                                    asyncio.create_task(delayed_sync())
                                    print("[NETWORK] Запланирована отложенная синхронизация через 5 секунд")
                                else:
                                    print("[NETWORK] Сессия не ONLINE или синхронизация уже отправлена, пропускаем")

                            if op == 19 and cmd == 1:
                                # Получен список чатов
                                chats = payload.get("chats", [])
                                print(f"[NETWORK] Получено чатов: {len(chats)}")
                                for c in chats:
                                    cid = str(c.get("id") or c.get("chatId"))
                                    last_m = c.get("lastMessage", {})
                                    if last_m.get("text"):
                                        # Пытаемся получить ID отправителя из sender
                                        sender = last_m.get("sender")
                                        auth_id = ""
                                        if isinstance(sender, dict):
                                            auth_id = str(sender.get("id") or sender.get("userId") or "")
                                        elif isinstance(sender, (str, int)):
                                            auth_id = str(sender)
                                        else:
                                            # fallback на authorId (устаревшее)
                                            auth_id = str(last_m.get("authorId") or "")
                                        side = 'right' if auth_id == MY_ID else 'left'
                                        title = c.get("title")
                                        name = title if title else f"Чат {cid}"
                                        self.save_to_cache(cid, last_m["text"], side, name=name)
                                        print(f"[NETWORK] Сохранён чат {cid}: {name} (side={side}, auth_id={auth_id})")
                                    # Запросить историю чата (первые 20 сообщений) - отключено для экономии запросов
                                    # asyncio.create_task(self.request_chat_history(ws, cid))

                            if op == 128 and cmd == 0:
                                # Новое сообщение
                                msg_p = payload.get("message", {})
                                cid = str(payload.get("chatId"))
                                text = msg_p.get("text")
                                if text:
                                    # Пытаемся получить ID отправителя из sender
                                    sender = msg_p.get("sender")
                                    auth_id = ""
                                    if isinstance(sender, dict):
                                        auth_id = str(sender.get("id") or sender.get("userId") or "")
                                    elif isinstance(sender, (str, int)):
                                        auth_id = str(sender)
                                    else:
                                        # fallback на authorId (устаревшее)
                                        auth_id = str(msg_p.get("authorId") or "")
                                    side = 'right' if auth_id == MY_ID else 'left'
                                    message_id = msg_p.get("id")
                                    timestamp = msg_p.get("time")
                                    if timestamp:
                                        timestamp = timestamp / 1000  # мс -> секунды
                                    else:
                                        timestamp = time.time()
                                    print(f"[NETWORK] Новое сообщение в чате {cid}: {text[:30]}... (side={side}) auth_id={auth_id}, MY_ID={MY_ID}, sender={sender}, id={message_id}")
                                    self.save_to_cache(cid, text, side, message_id=message_id)
                                    # Если это текущий чат, показать в UI (только если приложение не в паузе)
                                    if self.current_chat_id == cid and not self.app_paused:
                                        Clock.schedule_once(lambda dt: self.add_message_to_ui(text, side, timestamp=timestamp))

                            if op == 20 and cmd == 1:
                                # Получена история чата
                                if payload is None:
                                    print("[NETWORK] Ответ истории чата с пустым payload, пропускаем")
                                    continue
                                messages = payload.get("messages", [])
                                cid = str(payload.get("chatId"))
                                print(f"[NETWORK] Получена история чата {cid}: {len(messages)} сообщений")
                                for idx, msg in enumerate(messages):
                                    text = msg.get("text")
                                    if text:
                                        # Пытаемся получить ID отправителя из sender
                                        sender = msg.get("sender")
                                        auth_id = ""
                                        if isinstance(sender, dict):
                                            auth_id = str(sender.get("id") or sender.get("userId") or "")
                                        elif isinstance(sender, (str, int)):
                                            auth_id = str(sender)
                                        else:
                                            # fallback на authorId (устаревшее)
                                            auth_id = str(msg.get("authorId") or "")
                                        side = 'right' if auth_id == MY_ID else 'left'
                                        message_id = msg.get("id")
                                        timestamp = msg.get("time")
                                        if timestamp:
                                            timestamp = timestamp / 1000  # мс -> секунды
                                        else:
                                            timestamp = time.time()
                                        if idx == 0:
                                            print(f"[NETWORK] История: auth_id={auth_id}, MY_ID={MY_ID}, side={side}, sender={sender}, id={message_id}")
                                        self.save_to_cache(cid, text, side, message_id=message_id)
                                # Если это текущий чат, обновить UI (только если приложение не в паузе)
                                if self.current_chat_id == cid and not self.app_paused:
                                    Clock.schedule_once(lambda dt: self.reload_chat_history(cid))

                            if op == 7 and cmd == 1:
                                # Ответ на запрос истории (возможно, содержит список сообщений)
                                if payload is None:
                                    print("[NETWORK] Ответ opcode 7 с пустым payload, пропускаем")
                                    continue
                                messages = payload.get("messages", [])
                                cid = str(payload.get("chatId"))
                                print(f"[NETWORK] Получен ответ истории (opcode 7) для чата {cid}: {len(messages)} сообщений")
                                for idx, msg in enumerate(messages):
                                    text = msg.get("text")
                                    if text:
                                        # Пытаемся получить ID отправителя из sender
                                        sender = msg.get("sender")
                                        auth_id = ""
                                        if isinstance(sender, dict):
                                            auth_id = str(sender.get("id") or sender.get("userId") or "")
                                        elif isinstance(sender, (str, int)):
                                            auth_id = str(sender)
                                        else:
                                            # fallback на authorId (устаревшее)
                                            auth_id = str(msg.get("authorId") or "")
                                        side = 'right' if auth_id == MY_ID else 'left'
                                        message_id = msg.get("id")
                                        timestamp = msg.get("time")
                                        if timestamp:
                                            timestamp = timestamp / 1000  # мс -> секунды
                                        else:
                                            timestamp = time.time()
                                        if idx == 0:
                                            print(f"[NETWORK] История (opcode 7): auth_id={auth_id}, MY_ID={MY_ID}, side={side}, sender={sender}, id={message_id}")
                                        self.save_to_cache(cid, text, side, message_id=message_id)
                                # Если это текущий чат, обновить UI (только если приложение не в паузе)
                                if self.current_chat_id == cid and not self.app_paused:
                                    Clock.schedule_once(lambda dt: self.reload_chat_history(cid))

                            if op == 53 and cmd == 1:
                                # Ответ синхронизации пропущенных событий
                                if payload is None:
                                    print("[NETWORK] Ответ opcode 53 с пустым payload, пропускаем")
                                    continue
                                marker = payload.get("marker", 0)
                                self.sync_marker = marker
                                self.session_ready = True
                                print(f"[NETWORK] Синхронизация успешна, marker={marker}, сессия готова для запроса истории")

                            if op == 180 and cmd == 1:
                                # Обновление реакций на сообщения
                                reactions_data = payload.get("messagesReactions", {})
                                print(f"[NETWORK] Получены реакции для {len(reactions_data)} сообщений")
                                for msg_id_str, react_obj in reactions_data.items():
                                    # react_obj может быть {} или содержать counters
                                    counters = react_obj.get("counters", [])
                                    if counters:
                                        # Преобразуем в список строк реакций
                                        reactions = []
                                        for c in counters:
                                            emoji = c.get("reaction")
                                            count = c.get("count", 1)
                                            reactions.append(f"{emoji} {count}")
                                        # Обновляем реакции в кэше
                                        self.update_reactions(msg_id_str, reactions)
                                        print(f"[NETWORK] Реакции для сообщения {msg_id_str}: {reactions}")

                            if op == 49 and cmd == 1:
                                # Список сообщений (возможно, новые сообщения или обновления)
                                if payload is None:
                                    print("[NETWORK] Ответ opcode 49 с пустым payload, пропускаем")
                                    continue
                                messages = payload.get("messages", [])
                                print(f"[NETWORK] Получено сообщений (opcode 49): {len(messages)}")
                                # Отладочная информация о chatId из payload
                                payload_chat_id = payload.get("chatId")
                                print(f"[DEBUG] payload chatId: {payload_chat_id}, current_chat_id: {self.current_chat_id}")
                                need_reload = False
                                # Сообщения приходят от новых к старым, переворачиваем для правильного порядка
                                for msg in reversed(messages):
                                    text = msg.get("text", "")
                                    attaches = msg.get("attaches", [])
                                    # Если текст пустой, но есть вложения, создаём описание
                                    if not text and attaches:
                                        attach_types = []
                                        for attach in attaches:
                                            attach_type = attach.get("type", "unknown")
                                            attach_types.append(attach_type)
                                        text = f"[вложение: {', '.join(attach_types)}]"
                                    elif not text:
                                        # Если нет ни текста, ни вложений, пропускаем
                                        continue
                                    
                                    cid = str(msg.get("chatId") or payload.get("chatId") or "")
                                    if not cid:
                                        continue
                                    print(f"[DEBUG] Обработка сообщения cid={cid}, current_chat_id={self.current_chat_id}, совпадение? {self.current_chat_id == cid}")
                                    if self.current_chat_id == cid:
                                        need_reload = True
                                    sender = msg.get("sender")
                                    auth_id = ""
                                    if isinstance(sender, dict):
                                        auth_id = str(sender.get("id") or sender.get("userId") or "")
                                    elif isinstance(sender, (str, int)):
                                        auth_id = str(sender)
                                    else:
                                        auth_id = str(msg.get("authorId") or "")
                                    side = 'right' if auth_id == MY_ID else 'left'
                                    message_id = msg.get("id")
                                    # Сохраняем с ID
                                    self.save_to_cache(cid, text, side, message_id=message_id)
                                    print(f"[DEBUG] Сообщение сохранено в кэш для чата {cid}, message_id={message_id}")
                                    # Если это текущий чат, показать в UI (только если приложение не в паузе)
                                    if self.current_chat_id == cid and not self.app_paused:
                                        print(f"[DEBUG] Добавляем сообщение в UI для текущего чата")
                                        Clock.schedule_once(lambda dt: self.add_message_to_ui(text, side, timestamp=msg.get("time")/1000 if msg.get("time") else time.time()))
                                    else:
                                        print(f"[DEBUG] Сообщение не добавлено в UI (текущий чат {self.current_chat_id}, приложение в паузе? {self.app_paused})")
                                
                                # После обработки всех сообщений, если есть сообщения для текущего чата, перезагрузить историю
                                if need_reload and not self.app_paused:
                                    print(f"[DEBUG] Запланирована перезагрузка истории для текущего чата {self.current_chat_id}")
                                    Clock.schedule_once(lambda dt: self.reload_chat_history(self.current_chat_id))

                            # Ответы на служебные команды
                            if cmd == 0 and op != 1:
                                await ws.send(json.dumps({"ver": 11, "cmd": 1, "seq": data.get('seq'), "opcode": op, "payload": {}}))
                            if op == 1 and cmd == 0:
                                await ws.send(json.dumps({"ver": 11, "cmd": 1, "seq": data.get('seq'), "opcode": 1, "payload": {"interactive": True}}))

                            # Проверить запрос истории после обработки сообщения (если файл существует)
                            await self.check_history_request(ws)

                        except asyncio.TimeoutError:
                            os.utime(CACHE_FILE, None)
                            await self.check_send(ws)
                            await self.check_history_request(ws)
                        except ConnectionClosed:
                            print("[NETWORK] Соединение закрыто сервером, переподключение...")
                            break  # выходим из внутреннего цикла, чтобы переподключиться

            except Exception as e:
                self.network_state = "error"
                error_msg = f"Сетевая ошибка: {e}\n{traceback.format_exc()}"
                print(error_msg)
                # Запись в лог-файл
                log_file = os.path.join(os.path.dirname(CACHE_FILE), "maxa_log.txt")
                try:
                    with open(log_file, 'a', encoding='utf-8') as f:
                        f.write(error_msg + '\n')
                except:
                    pass
                # Увеличиваем счётчик попыток
                self.reconnect_attempt += 1
                # Экспоненциальная задержка с ограничением (макс 60 секунд)
                delay = min(self.reconnect_delay * (2 ** (self.reconnect_attempt - 1)), 60)
                # Если приложение в паузе, увеличиваем задержку дополнительно
                if self.app_paused:
                    delay = max(delay, 30)  # минимум 30 секунд в паузе
                    print(f"[NETWORK] Приложение в паузе, ждём {delay} сек.")
                else:
                    print(f"[NETWORK] Переподключение через {delay} сек. (попытка {self.reconnect_attempt})")
                await asyncio.sleep(delay)
                self.network_state = "offline"

    async def check_send(self, ws):
        """Проверить и отправить pending сообщения"""
        if os.path.exists(PENDING_FILE):
            print(f"[NETWORK] Найден pending файл: {PENDING_FILE}")
            try:
                with open(PENDING_FILE, 'r', encoding='utf-8') as f:
                    cmd = json.load(f)
                print(f"[NETWORK] Данные для отправки: {cmd}")
                os.remove(PENDING_FILE)
                payload = {
                    "chatId": int(cmd["chatId"]),
                    "message": {"text": cmd["text"], "type": "TEXT", "cid": random.getrandbits(31)}
                }
                seq = self.next_seq()
                await ws.send(json.dumps({
                    "ver": 11, "cmd": 0, "seq": seq,
                    "opcode": 64, "payload": payload
                }))
                print(f"[NETWORK] Отправлено сообщение в чат {cmd['chatId']}: {cmd['text'][:30]}..., seq={seq}")
                self.save_to_cache(str(cmd["chatId"]), cmd["text"], 'right')
            except Exception as e:
                print(f"Ошибка отправки pending: {e}")

    def save_to_memory_cache(self, cid, text, side, message_id=None, timestamp=None, reactions=None):
        """Сохранить сообщение в кэш памяти (chat_history_cache)"""
        cid_str = str(cid)
        if cid_str not in self.chat_history_cache:
            self.chat_history_cache[cid_str] = []
        
        hist = self.chat_history_cache[cid_str]
        
        # Поиск существующего сообщения по message_id
        existing_idx = -1
        if message_id:
            for i, msg in enumerate(hist):
                if msg.get("id") == message_id:
                    existing_idx = i
                    break
        
        # Если не нашли по id, ищем по тексту и стороне
        if existing_idx == -1:
            for i, msg in enumerate(hist):
                if msg.get("text") == text and msg.get("side") == side:
                    existing_idx = i
                    break
        
        msg_obj = {
            "text": text,
            "side": side,
            "time": timestamp if timestamp is not None else time.time()
        }
        if message_id:
            msg_obj["id"] = message_id
        if reactions:
            msg_obj["reactions"] = reactions
        elif existing_idx != -1 and "reactions" in hist[existing_idx]:
            # Сохраняем существующие реакции, если они есть
            msg_obj["reactions"] = hist[existing_idx]["reactions"]
        
        if existing_idx != -1:
            # Обновляем существующее сообщение
            hist[existing_idx] = msg_obj
        else:
            # Добавляем новое
            hist.append(msg_obj)
        
        # Ограничение количества сообщений в кэше памяти
        self.chat_history_cache[cid_str] = hist[-MAX_MESSAGES_PER_CHAT:]
        print(f"[MEMORY CACHE] Сохранено сообщение в чат {cid_str} (всего {len(hist)} сообщений)")

    def save_to_cache(self, cid, text, side, name=None, message_id=None, reactions=None):
        """Сохранить сообщение в файловый кэш (только для названий чатов)"""
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
        except:
            cache = {"names": {}, "history": {}}

        # Сохраняем только названия чатов в файл
        if name:
            current_name = cache["names"].get(str(cid))
            # Если имя уже есть и не является стандартным "Чат ...", не перезаписываем
            if current_name and current_name.startswith("Чат "):
                # Стандартное имя, можно заменить на новое (возможно, тоже стандартное)
                cache["names"][str(cid)] = name
            elif not current_name:
                # Имени ещё нет, сохраняем
                cache["names"][str(cid)] = name
            # Иначе current_name существует и не стандартное — оставляем как есть
        
        # История больше не сохраняется в файл, только в память
        # Но для обратной совместимости оставляем пустую структуру
        if "history" not in cache:
            cache["history"] = {}
        
        try:
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=4)
            os.utime(CACHE_FILE, None)
        except Exception as e:
            print(f"Ошибка записи кэша: {e}")
        
        # Также сохраняем в кэш памяти
        self.save_to_memory_cache(cid, text, side, message_id=message_id, reactions=reactions)

    async def check_history_request(self, ws):
        """Проверить и отправить запрос истории через Opcode 49 (новый метод) или Opcode 7"""
        if os.path.exists(HISTORY_REQUEST_FILE):
            try:
                with open(HISTORY_REQUEST_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                chat_id = data.get("chatId")
                request_type = data.get("type", "opcode7")  # по умолчанию opcode 7
                
                if not chat_id:
                    os.remove(HISTORY_REQUEST_FILE)
                    return
                
                if request_type == "opcode49":
                    # Используем opcode 49 для загрузки истории (как в веб-клиенте)
                    import time
                    current_time_ms = int(time.time() * 1000)
                    payload = {
                        "chatId": int(chat_id),
                        "from": current_time_ms,  # текущее время в мс
                        "forward": 0,  # сообщения после указанного времени
                        "backward": 50,  # количество сообщений для загрузки (увеличено с 30 до 50)
                        "getMessages": True
                    }
                    seq = self.next_seq()
                    await ws.send(json.dumps({
                        "ver": 11, "cmd": 0, "seq": seq,
                        "opcode": 49, "payload": payload
                    }))
                    print(f"[NETWORK] Отправлен запрос истории для чата {chat_id} (opcode 49) с backward=50, seq={seq}")
                else:
                    # Старый метод opcode 7
                    # Если синхронизация уже отправлена, запрос истории приведёт к ошибке "Must be NEW session"
                    if self.sync_sent:
                        print(f"[NETWORK] Синхронизация уже выполнена, запрос истории для чата {chat_id} невозможен. Удаляем файл запроса.")
                        os.remove(HISTORY_REQUEST_FILE)
                        return
                    payload = {
                        "chatId": int(chat_id),
                        "limit": 50,  # увеличено с 20 до 50
                        "offset": 0,
                        "marker": self.sync_marker,
                        "direction": "backward",
                        "type": "ALL"
                    }
                    seq = self.next_seq()
                    await ws.send(json.dumps({
                        "ver": 11, "cmd": 0, "seq": seq,
                        "opcode": 7, "payload": payload
                    }))
                    print(f"[NETWORK] Отправлен запрос истории для чата {chat_id} (opcode 7) с marker={self.sync_marker}, limit=50, seq={seq}")
                
                os.remove(HISTORY_REQUEST_FILE)
            except Exception as e:
                print(f"Ошибка обработки запроса истории: {e}")

    def update_reactions(self, message_id, reactions):
        """Обновить реакции для сообщения по его ID во всех чатах (в кэше памяти)"""
        updated = False
        for cid, hist in self.chat_history_cache.items():
            for msg in hist:
                if msg.get("id") == message_id:
                    msg["reactions"] = reactions
                    updated = True
                    break
            if updated:
                break

        if updated:
            print(f"[MEMORY CACHE] Обновлены реакции для сообщения {message_id}")
            
            # Также обновляем в файловом кэше для обратной совместимости
            try:
                with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
            except:
                cache = {"names": {}, "history": {}}
            
            file_updated = False
            for cid, hist in cache.get("history", {}).items():
                for msg in hist:
                    if msg.get("id") == message_id:
                        msg["reactions"] = reactions
                        file_updated = True
                        break
                if file_updated:
                    break
            
            if file_updated:
                try:
                    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                        json.dump(cache, f, ensure_ascii=False, indent=4)
                    os.utime(CACHE_FILE, None)
                except Exception as e:
                    print(f"Ошибка записи реакций в файловый кэш: {e}")

if __name__ == '__main__':
    SimpleMaxApp().run()