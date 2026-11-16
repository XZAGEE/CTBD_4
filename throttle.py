
import time
import threading
from collections import deque


class Throttle:
    def __init__(
        self,
        rate=3,
        interval=1,
        leading=True,
        trailing=False,
        overflow="drop"
    ):
        if rate < 1:
            raise ValueError("rate must be at least 1")

        if interval <= 0:
            raise ValueError("interval must be positive")

        if overflow not in ("queue", "drop"):
            raise ValueError("Unknown overflow strategy")

        self.rate = rate
        self.interval = interval
        self.leading = leading
        self.trailing = trailing
        self.overflow = overflow

        self.window_start = None
        self.calls_in_window = 0

        self.queue = deque()
        self.trailing_call = None

        self.timer = None
        self.lock = threading.Lock()

    def throttle(self, fn):
        def wrapper(*args, **kwargs):
            return self._invoke(fn, args, kwargs)

        return wrapper

    def _has_pending(self):
        return bool(self.queue) or self.trailing_call is not None

    def _schedule(self):
        if self.timer is not None:
            return

        delay = max(
            0,
            self.window_start + self.interval - time.monotonic()
        )

        self.timer = threading.Timer(delay, self._flush)
        self.timer.daemon = True
        self.timer.start()

    def _invoke(self, fn, args, kwargs):
        execute_now = False

        with self.lock:
            now = time.monotonic()

            if self.window_start is None:
                self.window_start = now

            # Перевірка завершення поточного часового вікна
            elif now >= self.window_start + self.interval:

                # Якщо залишилися відкладені виклики,
                # спочатку потрібно обробити їх
                if not self._has_pending():
                    self.window_start = now
                    self.calls_in_window = 0

            # Якщо є невиконані елементи черги
            # нові виклики додаються в її кінець
            if self.queue:
                self.queue.append((fn, args, kwargs))
                self._schedule()
                return None

            # Якщо очікує trailing-виклик,
            # зберігаємо останній отриманий виклик
            if self.trailing_call is not None:
                if self.trailing:
                    self.trailing_call = (fn, args, kwargs)

                self._schedule()
                return None

            # Перевіряємо можливість негайного виконання
            if self.leading and self.calls_in_window < self.rate:
                self.calls_in_window += 1
                execute_now = True

            else:
                # Черга зберігає всі надлишкові виклики
                if self.overflow == "queue":
                    self.queue.append((fn, args, kwargs))

                # Trailing зберігає лише останній виклик
                elif self.trailing:
                    self.trailing_call = (fn, args, kwargs)

                # Якщо обробка не передбачена,
                # надлишковий виклик відкидається
                else:
                    return None

                self._schedule()

        if execute_now:
            return fn(*args, **kwargs)

        return None

    def _flush(self):
        tasks = []

        with self.lock:
            self.timer = None

            now = time.monotonic()

            # Якщо часовий інтервал ще не завершився
            if now < self.window_start + self.interval:
                self._schedule()
                return

            # Починаємо нове часове вікно
            self.window_start = now
            self.calls_in_window = 0

            # Виконання елементів черги
            if self.overflow == "queue":
                while self.queue and self.calls_in_window < self.rate:
                    tasks.append(self.queue.popleft())
                    self.calls_in_window += 1

            # Виконання останнього відкладеного виклику
            elif self.trailing_call is not None:
                tasks.append(self.trailing_call)
                self.trailing_call = None
                self.calls_in_window += 1

            # Якщо залишилися невиконані виклики,
            # плануємо наступне виконання
            if self._has_pending():
                self._schedule()

        # Виконуємо функції поза блокуванням
        for fn, args, kwargs in tasks:
            fn(*args, **kwargs)
