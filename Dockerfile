# Используем официальный образ Python
FROM python:3.12.8

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install -r requirements.txt

# Копируем остальные файлы проекта
COPY . .

COPY credentials.json /app/credentials.json

ENV PYTHONPATH=/app

# Указываем порт, который будет использовать приложение
EXPOSE 5000

# Команда для запуска приложения
CMD ["python", "app/main.py"]