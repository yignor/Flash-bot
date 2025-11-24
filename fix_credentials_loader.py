#!/usr/bin/env python3
"""
Утилита для загрузки Google Sheets credentials разными способами
Решает проблему с многострочным JSON в .env файле
"""

import os
import json
from dotenv import load_dotenv, dotenv_values

def load_google_credentials():
    """Загружает Google Sheets credentials разными способами"""
    
    # Способ 1: Стандартная загрузка через os.getenv
    load_dotenv()
    creds = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    
    if creds and len(creds) > 10:  # Если загрузилось нормально
        try:
            json.loads(creds)
            return creds
        except:
            pass
    
    # Способ 2: Загрузка через dotenv_values (читает файл напрямую)
    env_values = dotenv_values(".env")
    creds = env_values.get("GOOGLE_SHEETS_CREDENTIALS", "")
    
    if creds and len(creds) > 10:
        try:
            json.loads(creds)
            return creds
        except:
            pass
    
    # Способ 3: Чтение .env файла напрямую и парсинг вручную
    try:
        with open(".env", "r", encoding="utf-8") as f:
            content = f.read()
        
        # Ищем GOOGLE_SHEETS_CREDENTIALS
        start_marker = "GOOGLE_SHEETS_CREDENTIALS="
        start_idx = content.find(start_marker)
        
        if start_idx != -1:
            # Находим начало значения (после =)
            value_start = start_idx + len(start_marker)
            
            # Пробуем найти конец JSON (ищем закрывающую скобку с правильным балансом)
            json_start = value_start
            # Пропускаем пробелы и кавычки в начале
            while json_start < len(content) and content[json_start] in [' ', '\t', '"', "'"]:
                json_start += 1
            
            # Ищем закрывающую скобку с правильным балансом
            brace_count = 0
            in_string = False
            escape_next = False
            json_end = json_start
            
            for i in range(json_start, len(content)):
                char = content[i]
                
                if escape_next:
                    escape_next = False
                    continue
                
                if char == '\\':
                    escape_next = True
                    continue
                
                if char == '"' and not escape_next:
                    in_string = not in_string
                    continue
                
                if not in_string:
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            json_end = i + 1
                            break
                
                # Если встретили новую строку с другим ключом (без отступа), останавливаемся
                if not in_string and char == '\n' and brace_count > 0:
                    # Проверяем следующую строку
                    next_line_start = i + 1
                    if next_line_start < len(content):
                        next_line = content[next_line_start:next_line_start+50].strip()
                        if next_line and not next_line.startswith(' ') and not next_line.startswith('\t') and '=' in next_line:
                            # Это новый ключ, но JSON не закрыт - возможно проблема
                            pass
            
            if json_end > json_start:
                creds = content[json_start:json_end]
                # Убираем кавычки если есть
                creds = creds.strip()
                if creds.startswith('"') and creds.endswith('"'):
                    creds = creds[1:-1]
                elif creds.startswith("'") and creds.endswith("'"):
                    creds = creds[1:-1]
                
                # Убираем экранированные символы
                creds = creds.replace('\\n', '\n').replace('\\r', '\r').replace('\\t', '\t')
                
                if len(creds) > 10:
                    try:
                        # Пробуем распарсить как есть
                        creds_dict = json.loads(creds)
                        return json.dumps(creds_dict)  # Нормализуем JSON
                    except json.JSONDecodeError:
                        # Исправляем переносы строк в private_key
                        import re
                        
                        # Находим private_key и заменяем переносы строк на \n
                        pattern = r'"private_key"\s*:\s*"([^"]*(?:\n[^"]*)*)"'
                        
                        def escape_newlines(match):
                            key_value = match.group(1)
                            # Экранируем переносы строк
                            escaped = key_value.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                            return f'"private_key": "{escaped}"'
                        
                        # Пробуем найти и заменить private_key
                        fixed_creds = re.sub(pattern, escape_newlines, creds, flags=re.DOTALL)
                        
                        # Если не сработало, пробуем более простой подход
                        if fixed_creds == creds:
                            # Ищем от "private_key" до закрывающей кавычки
                            start_idx = creds.find('"private_key"')
                            if start_idx != -1:
                                # Находим начало значения
                                colon_idx = creds.find(':', start_idx)
                                quote_start = creds.find('"', colon_idx)
                                if quote_start != -1:
                                    # Ищем конец значения (последняя " перед запятой или })
                                    quote_end = creds.rfind('"', quote_start + 1)
                                    # Ищем от конца назад до начала private_key
                                    for i in range(len(creds) - 1, quote_start, -1):
                                        if creds[i] == '"' and creds[i-1] != '\\':
                                            quote_end = i
                                            break
                                    
                                    if quote_end > quote_start:
                                        before = creds[:quote_start + 1]
                                        key_content = creds[quote_start + 1:quote_end]
                                        after = creds[quote_end:]
                                        
                                        # Экранируем переносы строк
                                        escaped_key = key_content.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                                        fixed_creds = before + escaped_key + after
                        
                        try:
                            creds_dict = json.loads(fixed_creds)
                            return json.dumps(creds_dict)
                        except json.JSONDecodeError:
                            # Возвращаем как есть - пусть players_manager попробует сам
                            return creds
    except Exception as e:
        pass
    
    # Способ 4: Ищем файл credentials.json
    possible_paths = [
        "credentials.json",
        "google-credentials.json",
        "service-account.json",
        os.path.expanduser("~/google-credentials.json"),
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    creds_dict = json.load(f)
                    return json.dumps(creds_dict)
            except Exception:
                pass
    
    return None

