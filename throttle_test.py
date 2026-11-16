
import pytest
import threading

from throttle import Throttle


# Тест 1. Обмеження кількості викликів
def test_rate_limit():
    results = []

    throttle = Throttle(
        rate=3,
        interval=1,
        leading=True,
        trailing=False,
        overflow="drop"
    )

    def task(value):
        results.append(value)

    limited_task = throttle.throttle(task)

    # Виконуємо 10 викликів поспіль
    for i in range(10):
        limited_task(i)

    assert len(results) == 3
    assert results == [0, 1, 2]


# Тест 2. Перевірка leading
def test_leading():
    results = []

    throttle = Throttle(
        rate=1,
        interval=0.2,
        leading=True,
        trailing=False
    )

    limited_task = throttle.throttle(
        lambda: results.append("called")
    )

    limited_task()

    # Перший виклик виконується одразу
    assert results == ["called"]


# Тест 3. Перевірка trailing
def test_trailing():
    results = []
    event = threading.Event()

    throttle = Throttle(
        rate=1,
        interval=0.1,
        leading=True,
        trailing=True
    )

    def task(value):
        results.append(value)

        if value == 3:
            event.set()

    limited_task = throttle.throttle(task)

    limited_task(1)
    limited_task(2)
    limited_task(3)

    # Очікуємо завершення відкладеного виклику
    assert event.wait(1)

    assert results == [1, 3]


# Тест 4. Перевірка черги
def test_queue_order():
    results = []
    event = threading.Event()

    throttle = Throttle(
        rate=2,
        interval=0.05,
        leading=True,
        trailing=False,
        overflow="queue"
    )

    def task(value):
        results.append(value)

        if value == 4:
            event.set()

    limited_task = throttle.throttle(task)

    for i in range(5):
        limited_task(i)

    # Чекаємо виконання всіх елементів черги
    assert event.wait(1)

    assert results == [0, 1, 2, 3, 4]


# Тест 5. Перевірка відсікання надлишкових викликів
def test_drop_excess_calls():
    results = []

    throttle = Throttle(
        rate=2,
        interval=1,
        leading=True,
        trailing=False,
        overflow="drop"
    )

    limited_task = throttle.throttle(
        lambda value: results.append(value)
    )

    for i in range(5):
        limited_task(i)

    assert results == [0, 1]


# Тест 6. Leading вимкнено, trailing увімкнено
def test_leading_false_trailing_true():
    results = []
    event = threading.Event()

    throttle = Throttle(
        rate=1,
        interval=0.1,
        leading=False,
        trailing=True,
        overflow="drop"
    )

    def task(value):
        results.append(value)
        event.set()

    limited_task = throttle.throttle(task)

    limited_task(1)
    limited_task(2)

    # Перший виклик не повинен виконатися одразу
    assert results == []

    # Після завершення інтервалу виконується останній
    assert event.wait(1)

    assert results == [2]
