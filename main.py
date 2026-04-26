import json
import os
import time
import asyncio
import threading
import random
import traceback
import sys
import requests
import io
import re
try:
    import qrcode
    QRCODE_AVAILABLE = True
except ImportError:
    QRCODE_AVAILABLE = False
    print("[WARNING] Библиотека qrcode не установлена. QR-код не будет генерироваться.")
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
from kivy.utils import platform, escape_markup
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
TOKEN = "An_Sx6HQ9HDijaNsT_MidUdzBLdmqndAjNC_CO3Bn81E9iz0OJv3tRVhFdxD-wrLlnSmUud-YaOrInZZxMgP5Dnt0e0VO-bunpN0xf6geMcFNrfqhZ2R8h9QBLJtlSYIE5Ufw_ziphzT-ixlZW4CvZoazvzzlXxDAT7RddrkAIo-bn956KU_ywODX2tbno9KCY5ZKu_x9TkNahptMVQifqQmMJcGXlYACEFqq2D7NOQ8bYJTTtjizYO2FhUwwrc5zvdSA0Mu9XQBykH2juQapm4z6bdGFyRRTdNVpbI21i9gCp9-5EoiC8-KjFOG3KzxsL50jaZ9Ix2nC3TTU3hFnntr-_MH39QJOdCN0I6fGjMdJees2W-LGyMCwQ8bniOAj3vN5_U-LQaCPEauU2eaOVB5WgGgbQ_otSyPz4BEM_MVf5diDMFkXqq0JIfFYaQQXtF8_3j9qyMvgLRBj6Twzsnxwvh5fePFYEjjW1tTPli_u8el9_t06C10E4QWWWlG18tvS-M-PF4r538fm0M8cdTGsAntYw1XmVRLmjH6bpIz7y8SEB0cWkcuK_EWgTkagv5JzBWsS1WWX89jTqxSSj8h4vAgAK2HvV-WPC-TPaZavk8mLruMruYFrPynFnf7AwWh7z96EMhzLNdCo6eSw0cdNFXoCEhsqQqOB4Kmu_NuNM1zMROtNBQsOiIHpxWpLTxSWja"
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
    """Упрощённый пузырь сообщения (без времени) с возможностью выделения текста"""
    def __init__(self, text, side='left', timestamp=None, **kwargs):
        super().__init__(**kwargs)
        self.size_hint = (None, None)
        bg = (0.07, 0.45, 0.8, 1) if side == 'right' else (0.2, 0.2, 0.25, 1)
        # Гибридный подход: Label для отображения текста, TextInput для выделения
        from kivy.uix.label import Label
        from kivy.uix.textinput import TextInput
        
        # Вычисляем размер текста через временный Label с обновлением текстуры
        temp_label = Label(
            text=text,
            font_size=sp(14),
            text_size=(dp(220), None),
            halign='left',
            valign='top',
            markup=True
        )
        temp_label.texture_update()
        text_size = temp_label.texture_size
        if text_size[0] > 0 and text_size[1] > 0:
            text_width = min(text_size[0], dp(220))
            text_height = text_size[1]
        else:
            # fallback - эмпирические значения
            text_width = dp(220)
            text_height = dp(20)
        
        # Отладочная печать
        print(f"[DEBUG] SimpleMessageBubble: text='{text[:30]}...', text_size={text_size}, text_width={text_width}, text_height={text_height}")
        
        # Учитываем внутренние отступы (padding left+right, top+bottom)
        pad_h = dp(12) * 2  # горизонтальные padding слева и справа
        pad_v = dp(8) * 2   # вертикальные padding сверху и снизу
        total_width = text_width + pad_h
        total_height = text_height + pad_v
        
        # Создаём Label для отображения текста (видимый)
        self.label = Label(
            text=text,
            size_hint=(None, None),
            halign='left',
            valign='top',
            font_size=sp(14),
            color=(1, 1, 1, 1),
            text_size=(text_width, None),
            markup=True
        )
        self.label.width = text_width
        self.label.height = text_height
        self.label.texture_update()
        
        # Очищаем разметку для TextInput (чтобы при копировании не было тегов)
        plain_text = re.sub(r'\[/?[^]]*\]', '', text)
        # Если после удаления тегов остались лишние пробелы, убираем их
        plain_text = plain_text.strip()
        if not plain_text:
            plain_text = text  # fallback (на случай, если весь текст состоял из тегов)
        
        # Создаём TextInput для выделения (невидимый текст, но видимое выделение)
        self.text_input = TextInput(
            text=plain_text,
            size_hint=(None, None),
            halign='left',
            font_size=sp(14),
            foreground_color=(0, 0, 0, 0),          # полностью прозрачный текст
            disabled_foreground_color=(0, 0, 0, 0),
            background_color=(0, 0, 0, 0),          # полностью прозрачный фон
            background_normal='',
            background_active='',
            background_disabled_normal='',
            border=(0, 0, 0, 0),
            readonly=True,
            multiline=True,
            cursor_blink=False,
            cursor_color=(0, 0, 0, 0),
            padding=(dp(12), dp(8)),                # внутренние отступы
            disabled=False,
            selection_color=(0.3, 0.6, 1, 0.5),     # цвет выделения
            focus=False,
            use_handles=False,                      # отключить ручки выделения
            use_bubble=True                         # оставить всплывающее меню копирования
        )
        self.text_input.text_size = (text_width, None)
        self.text_input.width = total_width
        self.text_input.height = total_height
        
        # Размер пузыря равен размеру TextInput (включая padding)
        self.size = (total_width, total_height)
        
        with self.canvas.before:
            Color(*bg)
            self.rect = RoundedRectangle(radius=[dp(12)])
        self.bind(pos=self._update_rect, size=self._update_rect)
        # Добавляем сначала Label, затем TextInput (TextInput будет поверх для событий)
        self.add_widget(self.label)
        self.add_widget(self.text_input)
        # Внутренние отступы (для расчёта размера пузыря)
        self.padding = [dp(12), dp(8)]  # горизонтальный, вертикальный

    def _update_rect(self, *args):
        self.rect.pos, self.rect.size = self.pos, self.size
        # Позиционируем TextInput вместе с пузырём
        self.text_input.pos = self.pos
        self.text_input.size = self.size
        # Позиционируем Label с учётом padding
        from kivy.metrics import dp
        pad_h = dp(12)
        pad_v = dp(8)
        self.label.pos = (self.pos[0] + pad_h, self.pos[1] + pad_v)
        # Размер Label уже установлен в __init__, но можно обновить, если размер пузыря изменился
        # (ширина текста не должна превышать ширину пузыря минус padding)
        max_label_width = self.size[0] - 2 * pad_h
        if self.label.width > max_label_width:
            self.label.width = max_label_width
            self.label.text_size = (max_label_width, None)
        # Обновляем text_size у TextInput, чтобы выделение работало правильно
        self.text_input.text_size = (max_label_width, None)
        # Обновляем текстуру Label, чтобы текст отобразился
        self.label.texture_update()

    def on_touch_down(self, touch):
        """Сбросить выделение TextInput при касании вне его области"""
        # Проверяем, находится ли касание внутри TextInput
        if self.text_input.collide_point(*touch.pos):
            # Касание внутри TextInput, позволяем стандартную обработку
            return super().on_touch_down(touch)
        else:
            # Касание вне TextInput, сбрасываем выделение
            try:
                if hasattr(self.text_input, 'cancel_selection'):
                    self.text_input.cancel_selection()
                # Снимаем фокус
                self.text_input.focus = False
            except Exception as e:
                print(f"[DEBUG] Ошибка при сбросе выделения: {e}")
            return super().on_touch_down(touch)

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
        self.chat_last_time = {}  # id -> timestamp последнего сообщения (из lastMessage)
        self.chat_is_group = {}  # id -> bool (групповой чат?)
        self.chat_participants = {}  # id -> list of user_ids (участники чата)
        self.user_names = {}  # user_id -> name (для подписи в групповых чатах)
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
        self.token_invalid = False  # флаг невалидности токена
        self.in_chat_interface = False  # флаг, что активен интерфейс чата (а не меню)
        # QR-авторизация
        self.qr_track_id = None
        self.qr_polling_interval = 5.0  # секунды
        self.qr_popup = None  # ссылка на popup с QR-кодом
        self.qr_auth_thread = None  # поток для асинхронной авторизации
        self.qr_auth_cancel = False  # флаг отмены потока авторизации

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
        from kivy.utils import platform, escape_markup
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
        self.root_layout = BoxLayout(orientation='vertical')

        # Верхняя панель с кнопкой Меню, заголовком, индикатором сети и кнопкой Connect
        top_panel = BoxLayout(size_hint_y=None, height=dp(40), padding=dp(5))
        self.menu_button = Button(text="Меню", size_hint_x=0.2)
        self.menu_button.bind(on_press=self.go_to_main_menu)
        title_label = Label(text="safe MAX", bold=True, size_hint_x=0.4)
        self.status_label = Label(text="OFFLINE", color=(1,0,0,1), bold=True, halign='left', size_hint_x=0.2)
        connect_btn = Button(text="Connect", size_hint_x=0.2)
        connect_btn.bind(on_press=self.reconnect_network)
        top_panel.add_widget(self.menu_button)
        top_panel.add_widget(title_label)
        top_panel.add_widget(self.status_label)
        top_panel.add_widget(connect_btn)

        # Контейнер для основного содержимого (меню или чат)
        self.main_content = BoxLayout(orientation='vertical')
        self.root_layout.add_widget(top_panel)
        self.root_layout.add_widget(self.main_content)

        # Показываем главное меню
        self.show_main_menu()

        # Запуск сетевого клиента
        Clock.schedule_once(lambda dt: self.start_network_thread(), 1)
        Clock.schedule_interval(self.update_network_status, 2)
        # Загрузить чаты из кэша
        Clock.schedule_once(lambda dt: self.load_chats_from_cache(), 0.5)

        return self.root_layout

    def go_to_main_menu(self, instance=None):
        """Вернуться в главное меню (если не в нём уже)"""
        if not self.in_chat_interface:
            # Уже в главном меню, ничего не делаем
            return
        self.show_main_menu()

    def show_main_menu(self):
        """Показать главное меню с кнопками"""
        # Сбрасываем флаг интерфейса чата
        self.in_chat_interface = False
        # Скрыть кнопку меню (в главном меню она не нужна)
        if hasattr(self, 'menu_button'):
            self.menu_button.opacity = 0.5
            self.menu_button.disabled = True
        # Очищаем контейнер
        self.main_content.clear_widgets()
        
        # Создаём вертикальный макет для кнопок меню
        menu_layout = BoxLayout(orientation='vertical', spacing=dp(20), padding=dp(40))
        
        # Кнопка "Чаты" (все чаты)
        chats_btn = Button(text="Чаты", size_hint_y=None, height=dp(60))
        chats_btn.bind(on_press=self.show_chat_list)
        menu_layout.add_widget(chats_btn)
        
        # Кнопка "Группы" (только групповые чаты)
        groups_btn = Button(text="Группы", size_hint_y=None, height=dp(60))
        groups_btn.bind(on_press=self.show_groups_list)
        menu_layout.add_widget(groups_btn)
        
        # Кнопка "Info" (информация)
        info_btn = Button(text="Info", size_hint_y=None, height=dp(60))
        info_btn.bind(on_press=self.show_info_popup)
        menu_layout.add_widget(info_btn)
        
        # Кнопка "Выход"
        exit_btn = Button(text="Выход", size_hint_y=None, height=dp(60))
        exit_btn.bind(on_press=self.exit_app)
        menu_layout.add_widget(exit_btn)
        
        self.main_content.add_widget(menu_layout)

    def switch_to_chat_interface(self):
        """Переключиться на интерфейс чата (панель чатов, сообщения, ввод)"""
        # Если интерфейс чата уже активен, просто обновляем заголовок
        if self.in_chat_interface:
            if self.current_chat_id:
                name = self.chat_list.get(self.current_chat_id, f"Чат {self.current_chat_id}")
                self.chat_selector.text = name
            return
        
        # Очищаем контейнер
        self.main_content.clear_widgets()
        
        # Панель чатов (кнопки выбора групп и контактов)
        chat_panel = BoxLayout(size_hint_y=None, height=dp(40), spacing=dp(5), padding=dp(5))
        self.chat_selector = Button(text="Группы", size_hint_x=0.5)
        self.chat_selector.bind(on_press=self.show_groups_list)
        contacts_button = Button(text="Контакты", size_hint_x=0.5)
        contacts_button.bind(on_press=self.show_contacts_list)
        chat_panel.add_widget(self.chat_selector)
        chat_panel.add_widget(contacts_button)
        
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
        
        # Добавляем всё в main_content
        self.main_content.add_widget(chat_panel)
        self.main_content.add_widget(messages_scroll)
        self.main_content.add_widget(input_panel)
        
        # Устанавливаем флаг
        self.in_chat_interface = True
        
        # Показать кнопку меню (активна в интерфейсе чата)
        if hasattr(self, 'menu_button'):
            self.menu_button.opacity = 1
            self.menu_button.disabled = False
        
        # Если есть выбранный чат, обновляем заголовок
        if self.current_chat_id:
            name = self.chat_list.get(self.current_chat_id, f"Чат {self.current_chat_id}")
            self.chat_selector.text = name

    def show_info_popup(self, instance):
        """Показать всплывающее окно с информацией"""
        text = "По всем вопросам пишите в Телеграмм:\n[ref=https://t.me/AST_Jason][color=0000ff]t.me/AST_Jason[/color][/ref]"
        content = BoxLayout(orientation='vertical', spacing=dp(10), padding=dp(20))
        # Используем Label с поддержкой разметки
        from kivy.uix.label import Label
        from kivy.core.text.markup import MarkupLabel
        label = Label(text=text, markup=True, halign='center', valign='middle')
        label.bind(on_ref_press=lambda instance, ref: self.open_url(ref))
        content.add_widget(label)
        close_btn = Button(text="Закрыть", size_hint_y=None, height=dp(40))
        popup = Popup(title="Информация", content=content, size_hint=(0.8, 0.5))
        close_btn.bind(on_press=popup.dismiss)
        content.add_widget(close_btn)
        popup.open()

    def open_url(self, url):
        """Открыть URL (заглушка)"""
        print(f"[INFO] Открыть URL: {url}")
        # В реальном приложении можно использовать webbrowser или Intent на Android
        # Пока просто логируем

    def exit_app(self, instance):
        """Выход из приложения (полное закрытие)"""
        self.stop()

    def load_chats_from_cache(self):
        """Загрузить список чатов из кэша и историю в память (для обратной совместимости)"""
        if not os.path.exists(CACHE_FILE):
            return
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            self.cache = cache
            self.chat_list = cache.get('names', {})
            
            # Загружаем флаги групповых чатов
            group_flags = cache.get('group_flags', {})
            for cid_str, is_group in group_flags.items():
                self.chat_is_group[cid_str] = bool(is_group)
            
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
        
        # Создать список чатов с временем последнего сообщения
        chat_items = []
        for cid, name in self.chat_list.items():
            last_time = self.chat_last_time.get(cid, 0)
            # Если нет времени в chat_last_time, попробовать получить из файлового кэша (для обратной совместимости)
            if last_time == 0:
                history = self.cache.get('history', {})
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

    def show_groups_list(self, instance):
        """Показать всплывающее окно со списком групп (только групповые чаты)"""
        self.load_chats_from_cache()
        if not self.chat_list:
            self.chat_selector.text = "Нет групп"
            return
        
        content = BoxLayout(orientation='vertical', spacing=dp(5), padding=dp(10))
        scroll = ScrollView()
        list_layout = BoxLayout(orientation='vertical', size_hint_y=None, spacing=dp(2))
        list_layout.bind(minimum_height=list_layout.setter('height'))
        
        # Создать список групп с временем последнего сообщения
        group_items = []
        for cid, name in self.chat_list.items():
            if not self.is_group_chat(cid):
                continue
            last_time = self.chat_last_time.get(cid, 0)
            # Если нет времени в chat_last_time, попробовать получить из файлового кэша (для обратной совместимости)
            if last_time == 0:
                history = self.cache.get('history', {})
                if cid in history and history[cid]:
                    last_msg = history[cid][-1]
                    last_time = last_msg.get('time', 0)
            group_items.append((cid, name, last_time))
        # Сортировка по времени убывания (самые свежие сверху)
        group_items.sort(key=lambda x: x[2], reverse=True)
        
        if not group_items:
            no_groups_label = Label(text="Нет групповых чатов", size_hint_y=None, height=dp(40))
            list_layout.add_widget(no_groups_label)
        
        for cid, name, _ in group_items:
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
        popup = Popup(title="Группы", content=content, size_hint=(0.8, 0.8))
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
        # Убедимся, что интерфейс чата активен
        self.switch_to_chat_interface()
        name = self.chat_list.get(chat_id_str, f"Чат {chat_id}")
        self.chat_selector.text = name
        print(f"[UI] Выбран чат: {chat_id} ({name})")
        # Если чат групповой, убедиться, что все участники добавлены в контакты
        if self.is_group_chat(chat_id_str):
            print(f"[UI] Чат {chat_id_str} является групповым, проверяем участников")
            if chat_id_str in self.chat_participants:
                participant_ids = self.chat_participants[chat_id_str]
                added = 0
                for user_id in participant_ids:
                    if user_id and user_id not in self.user_names:
                        self.user_names[user_id] = user_id  # сохраняем ID как имя
                        added += 1
                if added > 0:
                    print(f"[UI] Добавлено {added} участников в контакты")
            else:
                print(f"[UI] Участники чата {chat_id_str} неизвестны")
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

    def show_contacts_list(self, instance):
        """Показать всплывающее окно со списком контактов (пользователей)"""
        if not self.user_names:
            # Попробовать загрузить из кэша
            self.load_chats_from_cache()
        if not self.user_names:
            # Создать заглушку
            content = BoxLayout(orientation='vertical', spacing=dp(5), padding=dp(10))
            content.add_widget(Label(text="Нет контактов", halign='center'))
            # Кнопка добавления контакта
            add_btn = Button(text="Добавить контакт", size_hint_y=None, height=dp(40))
            close_btn = Button(text="Закрыть", size_hint_y=None, height=dp(40))
            popup = Popup(title="Контакты", content=content, size_hint=(0.8, 0.6))
            add_btn.bind(on_press=lambda btn: self.add_contact_dialog(popup))
            close_btn.bind(on_press=popup.dismiss)
            content.add_widget(add_btn)
            content.add_widget(close_btn)
            popup.open()
            return
        
        content = BoxLayout(orientation='vertical', spacing=dp(5), padding=dp(10))
        # Создаём popup сразу, чтобы он был доступен в лямбдах
        popup = Popup(title="Контакты", content=content, size_hint=(0.8, 0.8))
        
        # Кнопка добавления контакта перед списком
        add_btn = Button(text="Добавить контакт", size_hint_y=None, height=dp(40))
        add_btn.bind(on_press=lambda btn: self.add_contact_dialog(popup))
        content.add_widget(add_btn)
        
        scroll = ScrollView()
        list_layout = BoxLayout(orientation='vertical', size_hint_y=None, spacing=dp(2))
        list_layout.bind(minimum_height=list_layout.setter('height'))
        
        # Создать список контактов из user_names и личных чатов с временем последнего сообщения
        contact_dict = {}  # user_id -> (name, timestamp)
        # 1. Пользователи из user_names
        for user_id, name in self.user_names.items():
            # Пропускаем пустые имена
            if not name or name == 'Пользователь':
                continue
            timestamp = self.chat_last_time.get(user_id, 0)
            contact_dict[user_id] = (name, timestamp)
        # 2. Личные чаты (не групповые) из chat_list
        for cid, name in self.chat_list.items():
            if self.is_group_chat(cid):
                continue
            cid_str = str(cid)
            if not cid_str.isdigit():
                continue
            timestamp = self.chat_last_time.get(cid_str, 0)
            # Если контакт уже есть в словаре, обновляем имя только если текущее имя хуже (пустое или "Пользователь")
            if cid_str in contact_dict:
                existing_name, existing_ts = contact_dict[cid_str]
                # Предпочитаем имя из user_names (оно уже должно быть), но если имя из chat_list не "Чат X", можно обновить?
                # Пока оставляем существующее имя
                # Обновляем timestamp на максимальный
                if timestamp > existing_ts:
                    contact_dict[cid_str] = (existing_name, timestamp)
            else:
                contact_dict[cid_str] = (name, timestamp)
        
        # Преобразуем в список и сортируем по времени убыванию (сначала самые свежие)
        contact_items = [(uid, name, ts) for uid, (name, ts) in contact_dict.items()]
        contact_items.sort(key=lambda x: x[2], reverse=True)  # по timestamp убывание
        
        # Убираем timestamp для дальнейшей обработки
        contact_items = [(uid, name) for uid, name, _ in contact_items]
        
        for user_id, name in contact_items:
            row = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(40), spacing=dp(2))
            contact_btn = Button(text=f"{name} ({user_id})", size_hint_x=0.8,
                                  background_color=(0.2, 0.3, 0.2, 1))
            # При нажатии на контакт можно открыть чат с ним (если есть) или создать новый
            # Пока просто закрываем popup
            contact_btn.bind(on_release=lambda btn, uid=user_id: (self.switch_to_chat_with_user(uid), popup.dismiss()))
            edit_btn = Button(text="✏️", size_hint_x=0.2, font_size=sp(20))
            edit_btn.bind(on_release=lambda btn, uid=user_id: self.rename_contact_dialog(uid, popup))
            row.add_widget(contact_btn)
            row.add_widget(edit_btn)
            list_layout.add_widget(row)
        
        scroll.add_widget(list_layout)
        content.add_widget(scroll)
        
        close_btn = Button(text="Закрыть", size_hint_y=None, height=dp(40))
        close_btn.bind(on_press=popup.dismiss)
        content.add_widget(close_btn)
        popup.open()

    def switch_to_chat_with_user(self, user_id):
        """Переключиться на чат с пользователем (поиск или создание)"""
        print(f"[UI] Запрошен чат с пользователем {user_id}")
        user_id_str = str(user_id)
        # Проверить, есть ли чат с таким ID в chat_list (личный чат)
        if user_id_str in self.chat_list:
            # Чат существует, переключаемся
            self.switch_chat(user_id_str)
            return
        # Чат не найден, создаём запись в chat_list (личный чат)
        name = self.user_names.get(user_id_str, f"Пользователь {user_id_str}")
        self.chat_list[user_id_str] = name
        # Сохранить в кэш (только имя)
        self.save_to_cache(user_id_str, "", "left", name=name)
        # Пометить как не групповой чат
        self.chat_is_group[user_id_str] = False
        self.save_chat_group_flag(user_id_str, False)
        # Переключиться на чат
        self.switch_chat(user_id_str)

    def rename_contact_dialog(self, user_id, parent_popup):
        """Открыть диалог переименования контакта"""
        parent_popup.dismiss()  # закрыть список контактов
        current_name = self.user_names.get(str(user_id), f"Пользователь {user_id}")
        content = BoxLayout(orientation='vertical', spacing=dp(10), padding=dp(20))
        input_field = TextInput(text=current_name,
                                multiline=False, size_hint_y=None, height=dp(40))
        content.add_widget(input_field)
        
        btn_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(40), spacing=dp(5))
        save_btn = Button(text="Сохранить")
        cancel_btn = Button(text="Отмена")
        btn_layout.add_widget(save_btn)
        btn_layout.add_widget(cancel_btn)
        content.add_widget(btn_layout)
        
        popup = Popup(title=f"Переименовать контакт {user_id}", content=content,
                      size_hint=(0.8, 0.4))
        
        def save_name(instance):
            new_name = input_field.text.strip()
            if new_name:
                self.user_names[str(user_id)] = new_name
                # Сохранить в кэш
                print(f"[UI] Контакт {user_id} переименован в '{new_name}'")
                popup.dismiss()
                # Обновить список контактов
                self.show_contacts_list(None)
            else:
                input_field.hint_text = "Введите имя"
        
        def cancel(instance):
            popup.dismiss()
            self.show_contacts_list(None)
        
        save_btn.bind(on_press=save_name)
        cancel_btn.bind(on_press=cancel)
        popup.open()

    def add_contact_dialog(self, parent_popup):
        """Открыть диалог добавления нового контакта"""
        parent_popup.dismiss()  # закрыть список контактов
        content = BoxLayout(orientation='vertical', spacing=dp(10), padding=dp(20))
        # Поле для ID
        id_input = TextInput(hint_text="ID пользователя (число)", multiline=False, size_hint_y=None, height=dp(40))
        content.add_widget(id_input)
        # Поле для имени
        name_input = TextInput(hint_text="Имя (необязательно)", multiline=False, size_hint_y=None, height=dp(40))
        content.add_widget(name_input)
        
        btn_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(40), spacing=dp(5))
        save_btn = Button(text="Добавить")
        cancel_btn = Button(text="Отмена")
        btn_layout.add_widget(save_btn)
        btn_layout.add_widget(cancel_btn)
        content.add_widget(btn_layout)
        
        popup = Popup(title="Добавить контакт", content=content, size_hint=(0.8, 0.5))
        
        def save_contact(instance):
            user_id = id_input.text.strip()
            name = name_input.text.strip()
            if not user_id:
                id_input.hint_text = "Введите ID"
                return
            # Если имя не указано, используем ID как имя
            if not name:
                name = user_id
            # Проверяем, не существует ли уже контакт с таким ID
            if user_id in self.user_names:
                # Можно предложить переименовать, но просто обновим имя
                pass
            self.user_names[user_id] = name
            # Сохранить в кэш
            print(f"[UI] Добавлен контакт: {user_id} -> {name}")
            popup.dismiss()
            # Обновить список контактов
            self.show_contacts_list(None)
        
        def cancel(instance):
            popup.dismiss()
            self.show_contacts_list(None)
        
        save_btn.bind(on_press=save_contact)
        cancel_btn.bind(on_press=cancel)
        popup.open()

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
        
    def request_chat_list_update(self, instance=None):
        """Запросить обновление списка чатов (opcode 19) через WebSocket"""
        try:
            with open(HISTORY_REQUEST_FILE, 'w', encoding='utf-8') as f:
                json.dump({"type": "opcode19"}, f)
            print(f"[UI] Создан запрос обновления списка чатов: {HISTORY_REQUEST_FILE}")
        except Exception as e:
            print(f"[UI] Ошибка создания файла запроса списка чатов: {e}")
        
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
        
        # Используем универсальный метод обработки сообщений
        self.process_messages_list(all_messages, chat_id_str)
        
        # Дополнительно, если это текущий чат, можно вызвать reload_chat_history для гарантии порядка
        if chat_id_str == self.current_chat_id:
            self.reload_chat_history(chat_id_str)
        
        print(f"[UI] История чата {chat_id} обновлена через HTTP API, всего {len(self.chat_history_cache.get(chat_id_str, []))} сообщений в кэше")
    
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
        print(f"[UI] add_message_to_ui: text='{text[:50]}...', side={side}, timestamp={timestamp}")
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
        else:
            # Устанавливаем высоту контейнера равной высоте holder
            message_container.height = holder.height
        
        # Динамическое обновление высоты контейнера при изменении высоты holder
        def update_message_container_height(instance, value):
            if reactions:
                message_container.height = holder.height + reactions_label.height
            else:
                message_container.height = holder.height
        holder.bind(height=update_message_container_height)
        
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

    async def request_contact_info(self, ws, user_ids):
        """Запросить информацию о контактах (opcode 32)
        
        Args:
            ws: WebSocket соединение
            user_ids: список строковых ID пользователей
        """
        if not user_ids:
            return
        try:
            seq = self.next_seq()
            payload = {
                "contacts": [{"id": int(uid)} for uid in user_ids if uid.isdigit()]
            }
            await ws.send(json.dumps({
                "ver": 11, "cmd": 0, "seq": seq, "opcode": 32,
                "payload": payload
            }))
            print(f"[NETWORK] Запрошена информация о контактах: {user_ids}, seq={seq}")
        except Exception as e:
            print(f"[NETWORK] Ошибка запроса информации о контактах: {e}")

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
                                # Проверка на невалидный токен (любой opcode)
                                if any(keyword in error_msg.lower() for keyword in ['token', 'invalid', 'expired', 'недействителен', 'устарел']):
                                    self.token_invalid = True
                                    print(f"[NETWORK] Токен недействителен (op={op}), требуется обновление через QR-код")
                                    print(f"[NETWORK] Текст ошибки: {error_msg}")
                                    # Запланировать отображение UI
                                    Clock.schedule_once(lambda dt: self.show_token_error_ui(error_msg))
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
                                    
                                    # Определение имени чата
                                    title = c.get("title")
                                    if title:
                                        name = title
                                    else:
                                        # Попробовать получить участников
                                        users = c.get("users", [])
                                        if not users:
                                            users = c.get("participants", [])
                                        if users:
                                            # Извлечь имена участников (кроме себя) и сохранить ID участников
                                            names = []
                                            participant_ids = []
                                            for u in users:
                                                if isinstance(u, dict):
                                                    user_id = str(u.get("id") or u.get("userId") or "")
                                                    user_name = u.get("name")
                                                    # Сохранить ID участника
                                                    if user_id:
                                                        participant_ids.append(user_id)
                                                        # Сохранить имя пользователя в словарь (если есть имя, иначе сохранить ID как имя)
                                                        if user_name:
                                                            self.user_names[user_id] = user_name
                                                            print(f"[NETWORK] Сохранено имя пользователя: {user_id} -> {user_name}")
                                                        else:
                                                            # Сохранить ID как имя, чтобы отображался в контактах
                                                            if user_id not in self.user_names:
                                                                self.user_names[user_id] = user_id
                                                                print(f"[NETWORK] Сохранён участник (без имени): {user_id}")
                                                        if user_id != MY_ID and user_name:
                                                            names.append(user_name)
                                                else:
                                                    # u может быть строкой или числом (ID участника)
                                                    user_id = str(u)
                                                    participant_ids.append(user_id)
                                                    # Сохранить ID как имя
                                                    if user_id not in self.user_names:
                                                        self.user_names[user_id] = user_id
                                                        print(f"[NETWORK] Сохранён участник (без имени): {user_id}")
                                            # Сохранить список участников для этого чата
                                            self.chat_participants[cid] = participant_ids
                                            if names:
                                                name = ", ".join(names)
                                            else:
                                                name = f"Чат {cid}"
                                        else:
                                            name = f"Чат {cid}"
                                    
                                    # Сохранить имя в chat_list и кэше
                                    self.chat_list[cid] = name
                                    # Сохранить в файловый кэш (только имя)
                                    self.save_to_cache(cid, "", "left", name=name)
                                    # Сохранить время последнего сообщения (если есть)
                                    if last_m.get('time'):
                                        self.chat_last_time[cid] = last_m['time'] / 1000  # мс -> секунды
                                    elif cid in self.chat_last_time:
                                        # оставить старое значение
                                        pass
                                    else:
                                        self.chat_last_time[cid] = 0
                                    # Определить, является ли чат групповым (количество участников > 2)
                                    users = c.get("users", [])
                                    if not users:
                                        users = c.get("participants", [])
                                    participants_count = len(users) if isinstance(users, list) else 0
                                    print(f"[NETWORK] Чат {cid}: participants_count={participants_count}, users={users}")
                                    self.chat_is_group[cid] = participants_count > 1
                                    self.save_chat_group_flag(cid, participants_count > 1)
                                    print(f"[NETWORK] Чат {cid} является групповым? {self.chat_is_group[cid]}")
                                    
                                    # Если есть последнее сообщение, сохранить его
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
                                        self.save_to_cache(cid, last_m["text"], side, name=name)
                                        print(f"[NETWORK] Сохранён чат {cid}: {name} (side={side}, auth_id={auth_id})")
                                    else:
                                        print(f"[NETWORK] Чат {cid}: {name} (без последнего сообщения)")
                                    # Запросить историю чата (первые 20 сообщений) - отключено для экономии запросов
                                    # asyncio.create_task(self.request_chat_history(ws, cid))

                                # Сохранить имена пользователей в файловый кэш после обработки всех чатов
                                
                                # Собрать ID пользователей, у которых имя равно ID (неизвестные)
                                unknown_user_ids = []
                                for uid, uname in self.user_names.items():
                                    if uid == uname:  # имя равно ID, значит имя неизвестно
                                        unknown_user_ids.append(uid)
                                # Отправить запрос информации о контактах, если есть неизвестные
                                if unknown_user_ids:
                                    # Ограничим количество ID, чтобы не перегружать запрос
                                    batch = unknown_user_ids[:50]  # максимум 50 за раз
                                    asyncio.create_task(self.request_contact_info(ws, batch))
                                    print(f"[NETWORK] Запрошена информация о {len(batch)} неизвестных контактах: {batch}")

                            if op == 32 and cmd == 1:
                                # Ответ на запрос информации о контактах
                                contacts = payload.get("contacts", [])
                                print(f"[NETWORK] Получено контактов через opcode 32: {len(contacts)}")
                                for c in contacts:
                                    uid = str(c.get("id"))
                                    if not uid:
                                        continue
                                    # Берем первое имя из списка names
                                    names = c.get("names", [])
                                    name = uid  # fallback
                                    if names and isinstance(names, list) and len(names) > 0:
                                        # Отладочный вывод структуры names
                                        print(f"[NETWORK] names для {uid}: {names}")
                                        # Извлекаем имя из первого элемента списка
                                        first_name = names[0]
                                        if isinstance(first_name, dict):
                                            name = first_name.get("name", uid)
                                        else:
                                            name = str(first_name)
                                    else:
                                        print(f"[NETWORK] У контакта {uid} нет поля names или пустой список")
                                    # Сохраняем в словарь user_names
                                    if uid not in self.user_names or self.user_names[uid] != name:
                                        self.user_names[uid] = name
                                        print(f"[NETWORK] Сохранено имя пользователя {uid} -> {name}")
                                        # Запланировать обновление UI
                                        Clock.schedule_once(lambda dt, uid=uid, name=name: self.update_contact_ui(uid, name))
                                    # Также сохраняем в кэш контактов (если нужно)
                                    # self.save_user_names_to_cache() - отложим до конца обработки

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
                                    # Подпись имени отправителя в групповых чатах
                                    display_text = escape_markup(text)
                                    if side == 'left' and self.is_group_chat(cid):
                                        print(f"[NETWORK] Новое сообщение в групповом чате {cid}, sender={sender}, auth_id={auth_id}")
                                        sender_name = auth_id if auth_id else "Пользователь"
                                        # Пытаемся получить имя из sender (если это dict с полем name)
                                        if isinstance(sender, dict):
                                            sender_name = sender.get('name', sender_name)
                                            # Сохраняем имя в словарь user_names по auth_id, если оно не является fallback (auth_id или "Пользователь")
                                            if auth_id and sender_name != str(auth_id) and sender_name != "Пользователь":
                                                # Проверяем, изменилось ли имя
                                                if auth_id not in self.user_names or self.user_names[auth_id] != sender_name:
                                                    self.user_names[auth_id] = sender_name
                                                    print(f"[NETWORK] Сохранено имя пользователя {auth_id} -> {sender_name}")
                                            print(f"[NETWORK] Имя отправителя из dict: {sender_name}")
                                        elif isinstance(sender, str) and sender:
                                            # sender может быть строкой (имя), используем как есть
                                            sender_name = sender
                                            print(f"[NETWORK] Имя отправителя из строки: {sender_name}")
                                            # Сохраняем имя в словарь user_names, если оно не является fallback
                                            if auth_id and sender_name != str(auth_id) and sender_name != "Пользователь":
                                                if auth_id not in self.user_names or self.user_names[auth_id] != sender_name:
                                                    self.user_names[auth_id] = sender_name
                                                    print(f"[NETWORK] Сохранено имя пользователя {auth_id} -> {sender_name}")
                                        else:
                                            # sender не dict и не строка, возможно, это ID (число) или отсутствует
                                            # Ищем имя в словаре user_names по auth_id
                                            if auth_id and auth_id in self.user_names:
                                                sender_name = self.user_names[auth_id]
                                                print(f"[NETWORK] Имя отправителя из словаря user_names: {sender_name}")
                                            else:
                                                # Имя неизвестно, используем auth_id или "Пользователь"
                                                if auth_id:
                                                    sender_name = auth_id
                                                else:
                                                    sender_name = "Пользователь"
                                                print(f"[NETWORK] Имя отправителя неизвестно, используем '{sender_name}'")
                                        display_text = f"{sender_name}: {text}"
                                        print(f"[NETWORK] Итоговый текст: {display_text[:50]}")
                                    else:
                                        print(f"[NETWORK] Подпись не требуется: side={side}, chat_is_group={self.chat_is_group.get(cid, False)}")
                                    # Сохраняем в кэш текст с подписью (если группой)
                                    self.save_to_cache(cid, display_text, side, message_id=message_id)
                                    # Обновить время последнего сообщения для сортировки
                                    self.chat_last_time[cid] = timestamp
                                    # Если это текущий чат, показать в UI (только если приложение не в паузе)
                                    if self.current_chat_id == cid and not self.app_paused:
                                        Clock.schedule_once(lambda dt: self.add_message_to_ui(display_text, side, timestamp=timestamp))

                            if op == 20 and cmd == 1:
                                # Получена история чата
                                if payload is None:
                                    print("[NETWORK] Ответ истории чата с пустым payload, пропускаем")
                                    continue
                                messages = payload.get("messages", [])
                                cid = str(payload.get("chatId"))
                                print(f"[NETWORK] Получена история чата {cid}: {len(messages)} сообщений")
                                # Используем универсальный метод обработки сообщений
                                self.process_messages_list(messages, cid)

                            if op == 7 and cmd == 1:
                                # Ответ на запрос истории (возможно, содержит список сообщений)
                                if payload is None:
                                    print("[NETWORK] Ответ opcode 7 с пустым payload, пропускаем")
                                    continue
                                messages = payload.get("messages", [])
                                cid = str(payload.get("chatId"))
                                print(f"[NETWORK] Получен ответ истории (opcode 7) для чата {cid}: {len(messages)} сообщений")
                                # Используем универсальный метод обработки сообщений
                                self.process_messages_list(messages, cid)

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
                                # Список сообщений из WebSocket
                                if not payload:
                                    print("[NETWORK] Ответ opcode 49 с пустым payload")
                                    continue
                                
                                messages = payload.get("messages", [])
                                print(f"[NETWORK] Получено сообщений через WS (opcode 49): {len(messages)}")
                                
                                # Вызываем универсальный парсер
                                self.process_messages_list(messages, str(payload.get("chatId") or self.current_chat_id))

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

    def save_chat_group_flag(self, cid, is_group):
        """Сохранить флаг группового чата в файловый кэш"""
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
        except:
            cache = {"names": {}, "history": {}, "group_flags": {}}
        if "group_flags" not in cache:
            cache["group_flags"] = {}
        cache["group_flags"][str(cid)] = bool(is_group)
        try:
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=4)
            os.utime(CACHE_FILE, None)
        except Exception as e:
            print(f"Ошибка сохранения флага группового чата: {e}")

    def process_messages_list(self, messages, cid):
        """Универсальная обработка списка сообщений для UI и Кэша"""
        cid = str(cid)
        current_cid = str(self.current_chat_id)
        print(f"[PROCESS] Обработка {len(messages)} сообщений для чата {cid}, текущий чат {current_cid}")
        
        # Сортируем сообщения по времени (старые -> новые)
        def get_time(msg):
            t = msg.get('time') or msg.get('timestamp') or 0
            if t > 1_000_000_000_000:  # миллисекунды
                t = t / 1000
            return t
        messages_sorted = sorted(messages, key=get_time)
        
        last_date = None
        processed_ids = set()  # для дедупликации внутри одного вызова
        for msg in messages_sorted:
            msg_id = msg.get("id")
            if msg_id and msg_id in processed_ids:
                print(f"[PROCESS] Пропуск дубликата сообщения с ID {msg_id}")
                continue
            if msg_id:
                processed_ids.add(msg_id)
            
            text = msg.get("text", "")
            attaches = msg.get("attaches", [])
            
            if not text and attaches:
                types = [a.get("type", "unknown") for a in attaches]
                text = f"[вложение: {', '.join(types)}]"
            elif not text:
                continue

            # Определяем сторону (право/лево)
            sender = msg.get("sender")
            auth_id = ""
            if isinstance(sender, dict):
                auth_id = str(sender.get("id") or sender.get("userId") or "")
            elif isinstance(sender, (str, int)):
                auth_id = str(sender)
            else:
                # Пробуем различные поля для идентификации отправителя
                auth_id = str(msg.get("authorId") or msg.get("senderId") or "")
            side = 'right' if auth_id == MY_ID else 'left'
            
            # Если чат ещё не известен или помечен как не групповой, но сообщение от другого пользователя,
            # помечаем как групповой (fallback) - УБРАНО, используем is_group_chat
            # if side == 'left' and not self.chat_is_group.get(cid, False):
            #     self.chat_is_group[cid] = True
            #     self.save_chat_group_flag(cid, True)
            #     print(f"[PROCESS] Чат {cid} помечен как групповой (fallback)")
            
            # Подпись имени отправителя в групповых чатах
            display_text = escape_markup(text)
            if side == 'left' and self.is_group_chat(cid):
                print(f"[PROCESS] Чат {cid} является групповым, добавляем подпись. sender={sender}, auth_id={auth_id}")
                sender_name = auth_id if auth_id else "Пользователь"
                # Пытаемся получить имя из sender (если это dict с полем name)
                if isinstance(sender, dict):
                    sender_name = sender.get('name', sender_name)
                    # Сохраняем имя в словарь user_names по auth_id, если оно не является fallback (auth_id или "Пользователь")
                    if auth_id and sender_name != str(auth_id) and sender_name != "Пользователь":
                        # Проверяем, изменилось ли имя
                        if auth_id not in self.user_names or self.user_names[auth_id] != sender_name:
                            self.user_names[auth_id] = sender_name
                            print(f"[PROCESS] Сохранено имя пользователя {auth_id} -> {sender_name}")
                    print(f"[PROCESS] Имя отправителя из dict: {sender_name}")
                elif isinstance(sender, str) and sender:
                    # sender может быть строкой (имя), используем как есть
                    sender_name = sender
                    print(f"[PROCESS] Имя отправителя из строки: {sender_name}")
                    # Сохраняем имя в словарь user_names, если оно не является fallback (auth_id или "Пользователь")
                    if auth_id and sender_name != str(auth_id) and sender_name != "Пользователь":
                        if auth_id not in self.user_names or self.user_names[auth_id] != sender_name:
                            self.user_names[auth_id] = sender_name
                            print(f"[PROCESS] Сохранено имя пользователя {auth_id} -> {sender_name}")
                else:
                    # sender не dict и не строка, возможно, это ID (число) или отсутствует
                    # Ищем имя в словаре user_names по auth_id
                    if auth_id and auth_id in self.user_names:
                        sender_name = self.user_names[auth_id]
                        print(f"[PROCESS] Имя отправителя из словаря user_names: {sender_name}")
                    else:
                        # Имя неизвестно, используем auth_id или "Пользователь"
                        if auth_id:
                            sender_name = auth_id
                        else:
                            sender_name = "Пользователь"
                        print(f"[PROCESS] Имя отправителя неизвестно, используем '{sender_name}'")
                # Добавляем префикс с разметкой (жирный и фиолетовый)
                # Экранируем специальные символы разметки в имени и тексте
                escaped_sender = escape_markup(sender_name)
                escaped_text = escape_markup(text)
                # Фиолетовый цвет #DDA0DD (plum), жирный шрифт
                display_text = f"[b][color=#DDA0DD]{escaped_sender}:[/color][/b] {escaped_text}"
                print(f"[PROCESS] Итоговый текст с разметкой: {display_text[:70]}")
            else:
                print(f"[PROCESS] Подпись не требуется: side={side}, chat_is_group={self.chat_is_group.get(cid, False)}")
            
            # Сохраняем в кэш (текст с подписью для групповых чатов)
            self.save_to_cache(cid, display_text, side, message_id=msg.get("id"))

            # ОТРИСОВКА В UI (Исправленная лямбда)
            if cid == current_cid and not self.app_paused:
                msg_time = msg.get("time") or msg.get("timestamp")
                if msg_time:
                    # Если время в миллисекундах (больше 1e10), преобразуем в секунды
                    if msg_time > 1_000_000_000_000:  # > 31 688 年, явно миллисекунды
                        msg_time = msg_time / 1000
                else:
                    msg_time = time.time()
                
                # Определяем дату сообщения
                from datetime import datetime
                msg_date = datetime.fromtimestamp(msg_time).strftime('%Y-%m-%d')
                if msg_date != last_date:
                    # Добавить разделитель с датой
                    Clock.schedule_once(lambda dt, d=msg_date: self.add_date_separator(d))
                    last_date = msg_date
                
                # Фиксируем t=display_text и s=side в аргументах, чтобы они не менялись в цикле!
                Clock.schedule_once(lambda dt, t=display_text, s=side, tm=msg_time:
                                    self.add_message_to_ui(t, s, timestamp=tm))
                print(f"[PROCESS] Запланировано добавление сообщения в UI: {display_text[:30]}...")
            else:
                print(f"[PROCESS] Сообщение не добавлено в UI (cid={cid}, current={current_cid}, paused={self.app_paused})")

    def add_date_separator(self, date_str):
        """Добавить разделитель с датой в UI"""
        from kivy.uix.label import Label
        from kivy.uix.boxlayout import BoxLayout
        from kivy.metrics import dp
        
        # Контейнер для даты
        date_container = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(24))
        date_container.add_widget(Label(size_hint_x=0.2))
        date_label = Label(text=date_str, size_hint_x=0.6, font_size=dp(12),
                           color=(0.6, 0.6, 0.6, 1), halign='center', valign='middle')
        date_label.bind(size=date_label.setter('text_size'))
        date_container.add_widget(date_label)
        date_container.add_widget(Label(size_hint_x=0.2))
        
        self.messages_box.add_widget(date_container)
        print(f"[UI] Добавлен разделитель даты: {date_str}")

    async def check_history_request(self, ws):
        """Проверить и отправить запрос истории через Opcode 49 (новый метод) или Opcode 7"""
        if os.path.exists(HISTORY_REQUEST_FILE):
            try:
                with open(HISTORY_REQUEST_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                chat_id = data.get("chatId")
                request_type = data.get("type", "opcode7")  # по умолчанию opcode 7
                
                if request_type == "opcode19":
                    # Запрос обновления списка чатов
                    payload = {"token": TOKEN, "chatsCount": 40, "interactive": True}
                    seq = self.next_seq()
                    await ws.send(json.dumps({
                        "ver": 11, "cmd": 0, "seq": seq,
                        "opcode": 19, "payload": payload
                    }))
                    print(f"[NETWORK] Отправлен запрос обновления списка чатов (opcode 19), seq={seq}")
                    os.remove(HISTORY_REQUEST_FILE)
                    return
                
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

    def show_token_error_ui(self, error_msg):
        """Показать Popup с QR-кодом для авторизации"""
        from kivy.uix.popup import Popup
        from kivy.uix.boxlayout import BoxLayout
        from kivy.uix.label import Label
        from kivy.uix.image import Image
        from kivy.core.image import Image as CoreImage
        from kivy.uix.button import Button
        from kivy.clock import Clock
        
        content = BoxLayout(orientation='vertical', padding=20, spacing=15)
        
        # Заголовок
        title_label = Label(text="Отсканируйте QR-код для входа",
                           halign='center', font_size=sp(18), size_hint_y=0.1)
        content.add_widget(title_label)
        
        # Placeholder для QR-кода (будет обновлён после запроса к серверу)
        qr_image_widget = None
        if QRCODE_AVAILABLE:
            # Создаём пустое белое изображение-заглушку
            import io
            from PIL import Image as PILImage
            img = PILImage.new('RGB', (1, 1), color='white')
            buf = io.BytesIO()
            img.save(buf, format='PNG')
            buf.seek(0)
            img_texture = CoreImage(buf, ext='png').texture
            # QR-код по ширине экрана, сохраняет пропорции
            qr_image_widget = Image(texture=img_texture,
                                    size_hint=(1, None),
                                    height=300,  # временная высота, будет обновлена
                                    keep_ratio=True,
                                    allow_stretch=True)
            content.add_widget(qr_image_widget)
            # Надпись "Загрузка..."
            loading_label = Label(text="Загрузка QR-кода...",
                                 halign='center', color=(0.5,0.5,0.5,1),
                                 size_hint_y=0.05)
            content.add_widget(loading_label)
            # Сохраняем ссылки для последующего обновления
            self.qr_image_widget = qr_image_widget
            self.qr_loading_label = loading_label
        else:
            content.add_widget(Label(text="QR-код недоступен (библиотека qrcode не установлена)"))
        
        # Обработчик касания для разворачивания QR-кода (будет работать после обновления)
        def on_qr_touch(widget, touch):
            if widget.collide_point(*touch.pos) and hasattr(self, 'current_qr_link') and self.current_qr_link:
                # Создать увеличенный QR-код из текущей ссылки
                try:
                    import qrcode
                    import io
                    qr_full = qrcode.QRCode(
                        version=None,
                        error_correction=qrcode.constants.ERROR_CORRECT_L,
                        box_size=30,
                        border=6,
                    )
                    qr_full.add_data(self.current_qr_link)
                    qr_full.make(fit=True)
                    img_qr_full = qr_full.make_image(fill_color="black", back_color="white")
                    buf_full = io.BytesIO()
                    img_qr_full.save(buf_full, format='PNG')
                    buf_full.seek(0)
                    img_full = CoreImage(buf_full, ext='png')
                    full_image = Image(texture=img_full.texture, size_hint=(0.95, 0.95))
                    
                    # Popup на весь экран
                    full_content = BoxLayout(orientation='vertical', padding=20, spacing=10)
                    full_content.add_widget(Label(text="Увеличенный QR-код", halign='center', font_size=sp(16)))
                    full_content.add_widget(full_image)
                    full_content.add_widget(Label(text="Касание за пределами QR-кода закроет окно", halign='center', font_size=sp(12)))
                    
                    full_popup = Popup(title="", content=full_content, size_hint=(0.95, 0.95), auto_dismiss=True)
                    full_popup.open()
                except Exception as e:
                    print(f"[QR] Ошибка создания увеличенного QR-кода: {e}")
                return True
            return False
        
        if qr_image_widget:
            qr_image_widget.bind(on_touch_down=on_qr_touch)
        
        # Кнопка "Обновить QR-код"
        refresh_btn = Button(text="Обновить QR-код", size_hint_y=0.15)
        def on_refresh(instance):
            print("[UI] Запрос нового QR-кода")
            # Закрыть текущий popup и запустить новый поток
            popup.dismiss()
            # Установить флаг отмены текущего потока
            self.qr_auth_cancel = True
            # Запустить новый поток авторизации
            Clock.schedule_once(lambda dt: self.start_qr_auth_flow(None), 0.1)
            # Показать новый popup через небольшую задержку
            Clock.schedule_once(lambda dt: self.show_token_error_ui(error_msg), 0.2)
        refresh_btn.bind(on_press=on_refresh)
        content.add_widget(refresh_btn)
        
        popup = Popup(title="", content=content, size_hint=(0.9, 0.9))
        
        # Обработчик закрытия popup (отмена опроса)
        def on_popup_dismiss(instance):
            Clock.unschedule(self._poll_qr_status)
            self.qr_popup = None
            # Установить флаг отмены потока авторизации
            self.qr_auth_cancel = True
            # Попытаться остановить поток (неблокирующе)
            if self.qr_auth_thread and self.qr_auth_thread.is_alive():
                # Поток проверит флаг qr_auth_cancel и завершится
                pass
            print("[UI] Popup закрыт, опрос QR-кода отменён")
        popup.bind(on_dismiss=on_popup_dismiss)
        
        popup.open()
        # Запустить поток QR-авторизации
        Clock.schedule_once(lambda dt: self.start_qr_auth_flow(popup), 0.1)

    def is_group_chat(self, cid):
        """Определить, является ли чат групповым по его ID и сохранённым флагам"""
        cid_str = str(cid)
        # Эвристика: ID группового чата начинается с '-' или содержит минус
        if cid_str.startswith('-') or '-' in cid_str:
            return True
        # Если флаг уже известен, используем его
        if cid_str in self.chat_is_group:
            return self.chat_is_group[cid_str]
        # ID личного чата состоит из цифр (длина 9? но не гарантировано)
        # Если ID состоит только из цифр и длина <= 10, считаем личным
        if cid_str.isdigit() and len(cid_str) <= 10:
            return False
        # По умолчанию считаем личным (без подписи)
        return False

    def reconnect_network(self, instance=None):
        """Переподключиться к серверу (перезапустить сетевой поток)"""
        print("[NETWORK] Ручное переподключение")
        # Остановить старый поток, если он жив
        if self.network_thread and self.network_thread.is_alive():
            # Установить флаг остановки (если есть)
            pass
        # Запустить новый поток
        self.start_network_thread()

    def refresh_token_via_qr(self, popup=None):
        """Запуск QR-авторизации через WebSocket (совместимость)"""
        print("[TOKEN] Запуск QR-авторизации через WebSocket")
        # Просто запускаем основной поток авторизации
        self.start_qr_auth_flow(popup)
    
    def _poll_qr_status(self, dt):
        """Заглушка для опроса статуса QR-кода (реальный опрос происходит через WebSocket)"""
        # Увеличиваем счётчик попыток
        if not hasattr(self, 'qr_poll_attempts'):
            self.qr_poll_attempts = 0
        self.qr_poll_attempts += 1
        
        # Если это первая попытка, выведем сообщение и отменим дальнейшие вызовы
        if self.qr_poll_attempts == 1:
            print("[TOKEN] Опрос QR-кода через HTTP отключён, используется WebSocket протокол")
            Clock.unschedule(self._poll_qr_status)
        
        # Ничего не делаем
        return

    async def qr_auth_flow(self, popup=None):
        """Асинхронный поток для QR-авторизации по протоколу MAX (opcode 6, 288, 289)"""
        print("[QR-AUTH] Запуск потока QR-авторизации")
        # Сбросить флаг отмены
        self.qr_auth_cancel = False
        try:
            # Установка соединения с сервером WebSocket
            async with websockets.connect(
                WS_URI,
                ping_interval=30,
                ping_timeout=15,
                origin=ORIGIN,
            ) as ws:
                print("[QR-AUTH] WebSocket соединение установлено")
                
                # Шаг 1: Регистрация устройства (opcode 6)
                # Используем структуру из рабочего скрипта 1.py
                seq = self.next_seq()
                reg_payload = {
                    "ver": 11,
                    "cmd": 0,
                    "seq": seq,
                    "opcode": 6,
                    "payload": {
                        "userAgent": {
                            "deviceType": "WEB",
                            "osVersion": "Windows",
                            "deviceName": "Edge",
                            "appVersion": "26.4.7",
                            "locale": "ru"
                        },
                        "deviceId": DEVICE_ID
                    }
                }
                await ws.send(json.dumps(reg_payload))
                print(f"[QR-AUTH] Отправлен opcode 6 (регистрация устройства), seq={seq}")
                
                # Ждём ответ
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=10)
                    data = json.loads(response)
                    print(f"[QR-AUTH] Получен ответ на opcode 6: {data}")
                    
                    # Проверяем, успешен ли ответ
                    if data.get("cmd") == 3:  # ошибка
                        error_msg = data.get("payload", {}).get("localizedMessage", "Ошибка регистрации")
                        print(f"[QR-AUTH] Ошибка регистрации устройства: {error_msg}")
                        Clock.schedule_once(lambda dt: self.show_qr_error(f"Ошибка регистрации: {error_msg}", popup))
                        return
                    elif data.get("cmd") == 1:  # успех
                        print("[QR-AUTH] Устройство зарегистрировано успешно")
                    else:
                        print(f"[QR-AUTH] Неизвестный ответ: {data}")
                except asyncio.TimeoutError:
                    print("[QR-AUTH] Таймаут ожидания ответа на opcode 6")
                    Clock.schedule_once(lambda dt: self.show_qr_error("Таймаут регистрации устройства", popup))
                    return
                except websockets.exceptions.ConnectionClosed:
                    print("[QR-AUTH] Соединение закрыто сервером после регистрации")
                    Clock.schedule_once(lambda dt: self.show_qr_error("Сервер закрыл соединение", popup))
                    return
                
                # Шаг 2: Запрос QR-кода (opcode 288)
                seq = self.next_seq()
                qr_req_payload = {
                    "ver": 11,
                    "cmd": 0,
                    "seq": seq,
                    "opcode": 288,
                    "payload": {}  # пустой payload согласно логам
                }
                await ws.send(json.dumps(qr_req_payload))
                print(f"[QR-AUTH] Отправлен opcode 288 (запрос QR-кода), seq={seq}")
                
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=10)
                    data = json.loads(response)
                    print(f"[QR-AUTH] Получен ответ на opcode 288: {data}")
                except asyncio.TimeoutError:
                    print("[QR-AUTH] Таймаут ожидания ответа на opcode 288")
                    Clock.schedule_once(lambda dt: self.show_qr_error("Таймаут запроса QR-кода", popup))
                    return
                except websockets.exceptions.ConnectionClosed:
                    print("[QR-AUTH] Соединение закрыто сервером после запроса QR-кода")
                    Clock.schedule_once(lambda dt: self.show_qr_error("Сервер закрыл соединение", popup))
                    return
                
                if data.get("cmd") == 1:  # успешный ответ
                    qr_link = data.get("payload", {}).get("qrLink")
                    track_id = data.get("payload", {}).get("trackId")
                    polling_interval = data.get("payload", {}).get("pollingInterval", 5000)
                    
                    if qr_link and track_id:
                        print(f"[QR-AUTH] Получен qrLink: {qr_link}, trackId: {track_id}")
                        # Сохраняем trackId для опроса
                        self.qr_track_id = track_id
                        self.qr_polling_interval = polling_interval / 1000.0  # в секунды
                        
                        # Обновляем UI: генерируем QR-код из qrLink и показываем
                        Clock.schedule_once(lambda dt: self.update_qr_ui(qr_link, popup))
                        
                        # Шаг 3: Ожидание сканирования (opcode 289)
                        # Будем опрашивать сервер каждые polling_interval секунд
                        max_attempts = 60  # максимум 60 попыток (примерно 5 минут)
                        attempt = 0
                        while attempt < max_attempts and not self.qr_auth_cancel:
                            await asyncio.sleep(self.qr_polling_interval)
                            if self.qr_auth_cancel:
                                print("[QR-AUTH] Поток авторизации отменён")
                                break
                            attempt += 1
                            seq = self.next_seq()
                            poll_payload = {
                                "ver": 11,
                                "cmd": 0,
                                "seq": seq,
                                "opcode": 289,
                                "payload": {
                                    "trackId": track_id
                                }
                            }
                            await ws.send(json.dumps(poll_payload))
                            print(f"[QR-AUTH] Отправлен opcode 289 (опрос статуса), seq={seq}, попытка {attempt}")
                            
                            try:
                                response = await asyncio.wait_for(ws.recv(), timeout=10)
                                poll_data = json.loads(response)
                                print(f"[QR-AUTH] Получен ответ на opcode 289: {poll_data}")
                            except asyncio.TimeoutError:
                                print("[QR-AUTH] Таймаут ожидания ответа на opcode 289")
                                continue
                            
                            status = poll_data.get("payload", {}).get("status", {})
                            status_type = status.get("type", "")
                            expires_at = status.get("expiresAt")
                            token = poll_data.get("payload", {}).get("token")
                            login_available = status.get("loginAvailable")
                            
                            if status_type == "scanned" and token:
                                print(f"[QR-AUTH] QR-код отсканирован, получен токен: {token[:50]}...")
                                Clock.schedule_once(lambda dt: self.update_token_and_reconnect(token, popup))
                                break
                            elif status_type == "expired":
                                print("[QR-AUTH] QR-код истёк")
                                Clock.schedule_once(lambda dt: self.show_qr_error("QR-код истёк, запросите новый", popup))
                                break
                            elif login_available:
                                # Увеличиваем счётчик попыток получения токена после loginAvailable
                                if not hasattr(self, 'qr_login_attempts'):
                                    self.qr_login_attempts = 0
                                self.qr_login_attempts += 1
                                print(f"[QR-AUTH] QR-код отсканирован (loginAvailable=True), попытка {self.qr_login_attempts}/3")
                                
                                # Отправляем opcode 291 для получения токена через тот же WebSocket
                                seq = self.next_seq()
                                confirm_payload = {
                                    "ver": 11,
                                    "cmd": 0,
                                    "seq": seq,
                                    "opcode": 291,   # Команда на получение данных профиля и токена
                                    "payload": {
                                        "trackId": track_id  # тот же ID, что был у QR
                                    }
                                }
                                await ws.send(json.dumps(confirm_payload))
                                print(f"[QR-AUTH] Отправлен opcode 291, seq={seq}")
                                
                                try:
                                    response = await asyncio.wait_for(ws.recv(), timeout=10)
                                    confirm_data = json.loads(response)
                                    print(f"[QR-AUTH] Получен ответ на opcode 291 (структура): {confirm_data}")
                                    
                                    if confirm_data.get("cmd") == 1:  # успех
                                        # Извлекаем токен из нескольких возможных мест
                                        token = None
                                        # 1. payload.token (прямо в payload)
                                        token = confirm_data.get("payload", {}).get("token")
                                        # 2. payload.tokenAttrs.LOGIN.token
                                        if not token:
                                            token_attrs = confirm_data.get("payload", {}).get("tokenAttrs", {})
                                            token = token_attrs.get("LOGIN", {}).get("token")
                                        # 3. payload.access_token
                                        if not token:
                                            token = confirm_data.get("payload", {}).get("access_token")
                                        
                                        if token:
                                            print(f"[QR-AUTH] Получен токен через opcode 291: {token[:50]}...")
                                            Clock.schedule_once(lambda dt: self.update_token_and_reconnect(token, popup))
                                            break
                                        else:
                                            print("[QR-AUTH] Токен не найден в ответе opcode 291 (проверь структуру)")
                                            # Если превышен лимит попыток, показываем ошибку
                                            if self.qr_login_attempts >= 3:
                                                print("[QR-AUTH] Превышено количество попыток получения токена")
                                                Clock.schedule_once(lambda dt: self.show_qr_error("Не удалось получить токен после сканирования", popup))
                                                break
                                            # Продолжаем опрос
                                            continue
                                    else:
                                        print(f"[QR-AUTH] Ошибка ответа на opcode 291: {confirm_data}")
                                        # Продолжаем опрос
                                        continue
                                except asyncio.TimeoutError:
                                    print("[QR-AUTH] Таймаут ожидания ответа на opcode 291")
                                    continue
                                except websockets.exceptions.ConnectionClosed:
                                    print("[QR-AUTH] Соединение закрыто сервером при запросе токена")
                                    Clock.schedule_once(lambda dt: self.show_qr_error("Сервер закрыл соединение", popup))
                                    break
                            elif status_type == "pending" or expires_at:
                                # QR-код ещё не отсканирован, продолжаем ждать
                                print(f"[QR-AUTH] QR-код ожидает сканирования (истекает {expires_at})")
                                # Можно обновить UI с оставшимся временем
                                continue
                            else:
                                # Неизвестный статус, но возможно токен уже есть
                                if token:
                                    print(f"[QR-AUTH] Получен токен (неизвестный статус): {token[:50]}...")
                                    Clock.schedule_once(lambda dt: self.update_token_and_reconnect(token, popup))
                                    break
                                else:
                                    print("[QR-AUTH] Неизвестный статус, токен не найден")
                                    # Продолжаем опрос
                                    continue
                        else:
                            if attempt >= max_attempts:
                                print("[QR-AUTH] Превышено количество попыток опроса")
                                Clock.schedule_once(lambda dt: self.show_qr_error("Время ожидания истекло", popup))
                            if self.qr_auth_cancel:
                                print("[QR-AUTH] Опрос отменён пользователем")
                    else:
                        print("[QR-AUTH] В ответе отсутствуют qrLink или trackId")
                        # Показать ошибку в UI
                        Clock.schedule_once(lambda dt: self.show_qr_error("Не удалось получить QR-код", popup))
                else:
                    print(f"[QR-AUTH] Ошибка ответа на opcode 288: {data}")
                    Clock.schedule_once(lambda dt: self.show_qr_error("Ошибка запроса QR-кода", popup))
        except websockets.exceptions.ConnectionClosed as e:
            print(f"[QR-AUTH] Соединение закрыто сервером: {e}")
            Clock.schedule_once(lambda dt, err=e: self.show_qr_error(f"Сервер закрыл соединение: {err}", popup))
        except Exception as e:
            print(f"[QR-AUTH] Исключение в потоке QR-авторизации: {e}")
            traceback.print_exc()
            Clock.schedule_once(lambda dt, err=e: self.show_qr_error(f"Ошибка соединения: {err}", popup))

    def start_qr_auth_flow(self, popup=None):
        """Запустить асинхронный поток QR-авторизации в отдельном потоке"""
        print("[QR-AUTH] Запуск потока авторизации")
        # Сохраняем ссылку на popup
        self.qr_popup = popup
        # Создаём и запускаем поток
        import threading
        def run_flow():
            # Создаём новый event loop для этого потока
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.qr_auth_flow(popup))
            except Exception as e:
                print(f"[QR-AUTH] Ошибка в потоке: {e}")
            finally:
                loop.close()
        thread = threading.Thread(target=run_flow, daemon=True)
        thread.start()
        self.qr_auth_thread = thread

    def update_qr_ui(self, qr_link, popup):
        """Обновить UI popup с новым QR-кодом"""
        print(f"[UI] update_qr_ui вызван с qr_link={qr_link}, popup={popup}")
        if not popup or not popup.content:
            print("[UI] popup или content отсутствует")
            return
        # Сохраняем ссылку для обработчика разворачивания
        self.current_qr_link = qr_link
        
        # Найти виджет QR-кода в content (предполагаем, что это Image с size_hint (1, None))
        # Ищем первый виджет Image в content
        from kivy.uix.image import Image
        from kivy.core.image import Image as CoreImage
        from kivy.uix.label import Label
        import io
        import qrcode
        
        qr_image_widget = None
        loading_label = None
        for child in popup.content.children:
            if isinstance(child, Image):
                qr_image_widget = child
            if isinstance(child, Label) and child.text == "Загрузка QR-кода...":
                loading_label = child
        
        if qr_image_widget is None:
            print("[UI] Не найден виджет QR-кода для обновления")
            return
        
        # Генерируем QR-код из qr_link
        try:
            qr = qrcode.QRCode(
                version=None,
                error_correction=qrcode.constants.ERROR_CORRECT_L,
                box_size=15,
                border=4,
            )
            qr.add_data(qr_link)
            qr.make(fit=True)
            img_qr = qr.make_image(fill_color="black", back_color="white")
            buf = io.BytesIO()
            img_qr.save(buf, format='PNG')
            buf.seek(0)
            img = CoreImage(buf, ext='png')
            qr_image_widget.texture = img.texture
            # Устанавливаем квадратную высоту равной ширине
            if qr_image_widget.width > 0:
                qr_image_widget.height = qr_image_widget.width
            # Гарантируем, что изображение сохраняет пропорции и растягивается
            qr_image_widget.keep_ratio = True
            qr_image_widget.allow_stretch = True
            print(f"[UI] QR-код обновлён (ссылка: {qr_link})")
            
            # Удаляем надпись "Загрузка..."
            if loading_label and loading_label in popup.content.children:
                popup.content.remove_widget(loading_label)
            
            # Текстовое поле с данными QR-кода больше не используется
        except Exception as e:
            print(f"[UI] Ошибка генерации QR-кода: {e}")

    def show_qr_error(self, error_msg, popup):
        """Показать ошибку в popup"""
        if popup and popup.content:
            from kivy.uix.label import Label
            # Добавить Label с ошибкой
            popup.content.add_widget(Label(text=error_msg, color=(1,0,0,1)))
        print(f"[UI] Ошибка QR: {error_msg}")

    def update_contact_ui(self, user_id, name):
        """Обновить интерфейс контактов и групп после получения имени пользователя"""
        # Обновить список контактов, если он открыт
        if hasattr(self, 'contacts_list_widget') and self.contacts_list_widget:
            # Здесь можно обновить конкретный элемент списка контактов
            # Пока просто перезагрузим список контактов
            Clock.schedule_once(lambda dt: self.show_contacts_list(None))
        # Обновить список групп, если открыт
        if hasattr(self, 'groups_list_widget') and self.groups_list_widget:
            Clock.schedule_once(lambda dt: self.show_groups_list(None))
        print(f"[UI] Обновлён UI для пользователя {user_id} -> {name}")

    def update_token_and_reconnect(self, new_token, popup=None):
        """Обновить токен в конфигурации и переподключиться"""
        global TOKEN
        TOKEN = new_token
        # Сохранить в config.json
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
        except:
            config = {}
        config['token'] = new_token
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=4)
            print(f"[TOKEN] Токен обновлён в {CONFIG_FILE}")
        except Exception as e:
            print(f"[TOKEN] Ошибка сохранения токена: {e}")
        
        # Сбросить флаг невалидности
        self.token_invalid = False
        # Отменить опрос QR-кода
        Clock.unschedule(self._poll_qr_status)
        # Очистить ссылку на popup
        self.qr_popup = None
        # Закрыть popup если открыт
        if popup:
            popup.dismiss()
        # Перезапустить сетевой поток
        if self.network_thread and self.network_thread.is_alive():
            # Остановить старый поток (через флаг)
            # Пока просто запустим новый, старый завершится сам при разрыве соединения
            pass
        self.start_network_thread()
        print("[TOKEN] Сетевой поток перезапущен с новым токеном")


if __name__ == '__main__':
    SimpleMaxApp().run()
