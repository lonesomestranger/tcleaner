import asyncio

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import json
import logging
import os
import re
from functools import wraps
from typing import List, Optional, Set, Tuple, Union

from dotenv import load_dotenv
from pyrogram import Client, errors, raw, types, utils
from pyrogram.enums import ChatType

# True - входить в приватные чаты по ссылкам, чистить сообщения и выходить
# False - пропускать приватные чаты, в которых вы не состоите
JOIN_AND_LEAVE_PRIVATE_CHATS = True


try:
    import colorama

    colorama.init()
    _COLORAMA_AVAILABLE = True
except ImportError:
    _COLORAMA_AVAILABLE = False


logger = logging.getLogger("TCleaner")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()

if _COLORAMA_AVAILABLE:

    class ColoredFormatter(logging.Formatter):
        LEVEL_COLORS = {
            logging.DEBUG: colorama.Fore.CYAN,
            logging.INFO: colorama.Fore.GREEN,
            logging.WARNING: colorama.Fore.YELLOW,
            logging.ERROR: colorama.Fore.RED,
            logging.CRITICAL: colorama.Fore.MAGENTA,
        }
        RESET = colorama.Style.RESET_ALL

        def format(self, record):
            log_color = self.LEVEL_COLORS.get(record.levelno, colorama.Fore.WHITE)
            formatted_message = super().format(record)
            return f"{log_color}{formatted_message}{self.RESET}"

    formatter = ColoredFormatter("[%(levelname)s]: %(message)s")
else:
    formatter = logging.Formatter("[%(levelname)s]: %(message)s")

handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


CONFIG_FILE_KEYWORDS = "config.txt"
CONFIG_FILE_LINKS = "links_to_clean.txt"
SESSION_NAME = "pyrogram_deleter_session"

stats = {
    "total_checked_api": 0,
    "total_checked_manual": 0,
    "total_found_own": 0,
    "total_found_other": 0,
    "deleted_for_me": 0,
    "deleted_for_all": 0,
    "failed_to_delete_own": 0,
    "failed_revoke_but_deleted_for_me": 0,
    "attempted_delete_other": 0,
    "failed_to_delete_other": 0,
    "chats_processed": 0,
    "chats_failed": 0,
    "dialogs_found": 0,
    "dialogs_skipped_type": 0,
}


RETRYABLE_EXCEPTIONS = (errors.Timeout, ConnectionError)


def retry_on_exception(max_retries: int, delay: int):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except errors.FloodWait as e:
                    wait_time = e.value + 2
                    logger.warning(
                        f"Слишком много запросов. Ждем {wait_time} секунд..."
                    )
                    await asyncio.sleep(wait_time)
                    return await func(*args, **kwargs)
                except RETRYABLE_EXCEPTIONS as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Ошибка сети/сервера ({type(e).__name__}). Попытка {attempt + 1}/{max_retries}. Повтор через {delay} сек..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Превышено количество попыток. Ошибка: {e}")
                        return None
                except Exception as e:
                    logger.error(
                        f"В функции '{func.__name__}' произошла неисправимая ошибка: {type(e).__name__}: {e}"
                    )
                    return None
            return None

        return wrapper

    return decorator


async def get_user_choice(prompt: str, options: List[str]) -> str:
    print(f"\n{prompt}")
    for i, option in enumerate(options):
        print(f"  {i + 1}. {option}")
    while True:
        try:
            choice = input(f"Введите ваш выбор (1-{len(options)}): ").strip()
            index = int(choice) - 1
            if 0 <= index < len(options):
                return options[index]
            else:
                print("Неверный номер выбора.")
        except ValueError:
            print("Неверный ввод. Пожалуйста, введите число.")
        except (KeyboardInterrupt, EOFError):
            logger.warning("Операция отменена пользователем.")
            raise


def load_keywords(filename: str) -> List[str]:
    keywords = []
    if not os.path.exists(filename):
        logger.error(f"Файл конфигурации '{filename}' не найден!")
        return []
    try:
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line and not stripped_line.startswith("#"):
                    keywords.append(stripped_line.lower().replace('"', ""))
        logger.info(f"Загружено {len(keywords)} ключевых слов из '{filename}'.")
        return keywords
    except Exception as e:
        logger.error(f"Ошибка чтения ключевых слов из '{filename}': {e}")
        return []


async def get_target_chat_for_keywords(client: Client) -> Optional[types.Chat]:
    choice = await get_user_choice(
        "Выберите цель для поиска по ключевым словам:",
        ["Удалить из ВСЕХ личных чатов и бесед", "Удалить из КОНКРЕТНОГО чата"],
    )
    if choice == "Удалить из ВСЕХ личных чатов и бесед":
        return None
    elif choice == "Удалить из КОНКРЕТНОГО чата":
        while True:
            chat_input = input(
                "Введите Имя чата, @username, Номер телефона или ID чата: "
            ).strip()
            if not chat_input:
                continue
            try:
                entity: types.Chat = await client.get_chat(chat_input)
                entity_name = entity.title or entity.username or f"ID: {entity.id}"
                logger.info(
                    f"Найден чат: {entity_name} (ID: {entity.id}, Тип: {entity.type.name if entity.type else 'N/A'})"
                )
                confirm = input("Это верный чат? (да/нет): ").lower()
                if confirm == "да":
                    return entity
            except Exception as e:
                logger.error(f"Чат '{chat_input}' не найден или произошла ошибка: {e}")
            retry = input("Попробовать найти другой чат? (да/нет): ").lower()
            if retry != "да":
                raise Exception("Пользователь отменил выбор чата.")
    return None


async def delete_batch_own_messages(
    client: Client,
    chat_id: Union[int, str],
    chat_name_for_log: str,
    message_ids: List[int],
    revoke: bool,
) -> bool:
    global stats
    if not message_ids:
        return True
    action = "у всех" if revoke else "только у себя"
    count = len(message_ids)
    try:
        await client.delete_messages(
            chat_id=chat_id, message_ids=message_ids, revoke=revoke
        )
        logger.info(
            f"УСПЕШНО: Удалено {count} ВАШИХ сообщений {action} в '{chat_name_for_log}'"
        )
        if revoke:
            stats["deleted_for_all"] += count
        else:
            stats["deleted_for_me"] += count
        return True
    except errors.MessageDeleteForbidden:
        logger.warning(
            f"ЗАПРЕЩЕНО удалять {count} ВАШИХ сообщений {action} в '{chat_name_for_log}'."
        )
        if revoke:
            logger.info("Попытка удалить те же сообщения только у себя.")
            success_for_me = await delete_batch_own_messages(
                client, chat_id, chat_name_for_log, message_ids, revoke=False
            )
            if success_for_me:
                stats["failed_revoke_but_deleted_for_me"] += count
            else:
                stats["failed_to_delete_own"] += count
            return success_for_me
        else:
            stats["failed_to_delete_own"] += count
            return False
    except errors.FloodWait as e:
        wait_time = e.value + 5
        logger.warning(f"[FloodWait] при удалении. Ожидание {wait_time} секунд.")
        await asyncio.sleep(wait_time)
        return await delete_batch_own_messages(
            client, chat_id, chat_name_for_log, message_ids, revoke
        )
    except Exception as e:
        logger.error(f"НЕИЗВЕСТНАЯ ОШИБКА при удалении ВАШИХ сообщений: {e}")
        stats["failed_to_delete_own"] += count
        return False


async def attempt_delete_other_message(
    client: Client, chat_id: Union[int, str], chat_name_for_log: str, message_id: int
):
    global stats
    stats["attempted_delete_other"] += 1
    try:
        await client.delete_messages(
            chat_id=chat_id, message_ids=message_id, revoke=True
        )
        logger.info(
            f"УСПЕХ: Удалось удалить ЧУЖОЕ сообщение ID {message_id} у всех в '{chat_name_for_log}'."
        )
    except (
        errors.MessageDeleteForbidden,
        errors.RpcCallFail,
        errors.MessageAuthorRequired,
    ):
        stats["failed_to_delete_other"] += 1
    except Exception:
        stats["failed_to_delete_other"] += 1


URL_PATTERN_FOR_EXTRACTION = re.compile(
    r"https://t\.me/(?:[a-zA-Z0-9_]+|c/\d+|joinchat/[-_a-zA-Z0-9]+|\+[-_a-zA-Z0-9]+)(?:/\d+)?"
)
URL_PATTERN_FOR_PARSING = re.compile(
    r"https://t\.me/(?:(c/\d+)|(\+[-_a-zA-Z0-9]+|joinchat/[-_a-zA-Z0-9]+|joinchat/\+[-_a-zA-Z0-9]+)|([a-zA-Z0-9_]+))(?:/(\d+))?"
)


def extract_urls_from_file(filename: str) -> List[str]:
    try:
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
            urls = {
                match.group(0) for match in URL_PATTERN_FOR_EXTRACTION.finditer(content)
            }
            return list(urls)
    except FileNotFoundError:
        logger.error(f"Файл со ссылками '{filename}' не найден.")
        return []


@retry_on_exception(max_retries=3, delay=5)
async def get_chat_entity_from_link(
    url: str, app: Client
) -> Optional[Tuple[types.Chat, bool]]:
    match = URL_PATTERN_FOR_PARSING.match(url)
    if not match:
        return None
    private_chat_part, invite_hash, username, _ = match.groups()
    chat_identifier = (
        username
        or invite_hash
        or (int("-100" + private_chat_part.split("/")[1]) if private_chat_part else url)
    )
    try:
        chat = await app.get_chat(chat_identifier)
        return chat, False
    except (
        errors.InviteHashInvalid,
        errors.InviteHashExpired,
        errors.UserNotParticipant,
    ) as e:
        if JOIN_AND_LEAVE_PRIVATE_CHATS and invite_hash:
            logger.info(f"Попытка входа в приватный чат: {url}")
            try:
                clean_hash = (
                    invite_hash.replace("joinchat/", "").replace("+", "").strip()
                )
                joined_chat = await app.join_chat(clean_hash)
                logger.info(f"Успешно вошел в чат: '{joined_chat.title}'")
                return joined_chat, True
            except Exception as join_error:
                logger.error(
                    f"Не удалось войти в чат по ссылке {url}. Ошибка: {join_error}"
                )
                return None
        else:
            logger.warning(
                f"Ссылка недействительна или вы не участник: {url} ({type(e).__name__})"
            )
            return None
    except (
        errors.UsernameInvalid,
        errors.UsernameNotOccupied,
        errors.ChannelInvalid,
        errors.PeerIdInvalid,
    ):
        logger.warning(f"Ссылка недействительна, устарела или недоступна: {url}")
        return None
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при получении чата '{chat_identifier}': {e}"
        )
        raise


@retry_on_exception(max_retries=3, delay=5)
async def process_chat_for_link_cleaning(app: Client, initial_chat: types.Chat):
    target_chat: Optional[types.Chat] = None
    if initial_chat.linked_chat:
        target_chat = initial_chat.linked_chat
        logger.info(
            f"Найден канал '{initial_chat.title}'. Перехожу в связанный чат: '{target_chat.title}'"
        )
    elif initial_chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        target_chat = initial_chat
        logger.info(f"Найден чат: '{target_chat.title}'")
    else:
        logger.info(f"Объект '{initial_chat.title}' не является группой. Пропускаю.")
        return
    if not target_chat:
        logger.error(f"Не удалось определить целевой чат для '{initial_chat.title}'.")
        return
    try:
        messages_to_delete = [
            msg.id async for msg in app.search_messages(target_chat.id, from_user="me")
        ]
        if not messages_to_delete:
            logger.info("Моих сообщений в этом чате не найдено.")
            return
        logger.info(f"Найдено {len(messages_to_delete)} ваших сообщений для удаления.")
        for i in range(0, len(messages_to_delete), 100):
            chunk = messages_to_delete[i : i + 100]
            await delete_batch_own_messages(
                app, target_chat.id, target_chat.title, chunk, revoke=True
            )
            if len(messages_to_delete) > 100:
                await asyncio.sleep(1)
    except (errors.UserNotParticipant, errors.ChannelPrivate):
        logger.warning(
            f"Не могу искать сообщения в '{target_chat.title}'. Возможно, вы не участник."
        )
    except Exception as e:
        logger.error(f"Ошибка при удалении сообщений из '{target_chat.title}': {e}")
        raise


async def run_keyword_cleaning(client: Client):
    global stats
    keywords_config = load_keywords(CONFIG_FILE_KEYWORDS)
    if not keywords_config:
        return

    target_chat_obj = await get_target_chat_for_keywords(client)
    deletion_mode_choice = await get_user_choice(
        "Выберите режим удаления:",
        ["Удалить сообщения ТОЛЬКО У СЕБЯ", "Попытаться удалить сообщения У ВСЕХ"],
    )
    delete_for_everyone = deletion_mode_choice == "Попытаться удалить сообщения У ВСЕХ"

    dialogs_to_process: List[Tuple[Union[int, str], str, ChatType]] = []
    if target_chat_obj:
        dialogs_to_process.append(
            (
                target_chat_obj.id,
                target_chat_obj.title or target_chat_obj.username,
                target_chat_obj.type,
            )
        )
    else:
        logger.info("Получение списка всех диалогов...")
        async for dialog in client.get_dialogs():
            stats["dialogs_found"] += 1
            chat = dialog.chat
            if not chat:
                continue
            if chat.type in {ChatType.PRIVATE, ChatType.GROUP, ChatType.SUPERGROUP}:
                dialogs_to_process.append(
                    (chat.id, chat.title or chat.username, chat.type)
                )
            else:
                stats["dialogs_skipped_type"] += 1

    logger.info(
        f"Начинается обработка {len(dialogs_to_process)} диалогов по ключевым словам..."
    )
    my_id = (await client.get_me()).id

    for i, (chat_id, chat_name, chat_type) in enumerate(dialogs_to_process, 1):
        stats["chats_processed"] += 1
        logger.info(
            f"\n--- Обработка чата {i}/{len(dialogs_to_process)}: '{chat_name}' ---"
        )
        own_ids_to_delete: Set[int] = set()

        try:
            for keyword in keywords_config:
                async for message in client.search_messages(chat_id, query=keyword):
                    stats["total_checked_api"] += 1
                    if not message.from_user:
                        continue
                    if message.from_user.id == my_id:
                        own_ids_to_delete.add(message.id)
                    elif delete_for_everyone and chat_type == ChatType.PRIVATE:
                        await attempt_delete_other_message(
                            client, chat_id, chat_name, message.id
                        )

                if own_ids_to_delete:
                    logger.info(
                        f"Найдено {len(own_ids_to_delete)} ВАШИХ сообщений. Начинаю удаление..."
                    )
                    await delete_batch_own_messages(
                        client,
                        chat_id,
                        chat_name,
                        sorted(list(own_ids_to_delete)),
                        delete_for_everyone,
                    )
                    own_ids_to_delete.clear()
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Ошибка при обработке чата '{chat_name}': {e}")
            stats["chats_failed"] += 1


async def run_link_based_cleaning(client: Client):
    urls = extract_urls_from_file(CONFIG_FILE_LINKS)
    if not urls:
        logger.warning(f"Файл '{CONFIG_FILE_LINKS}' пуст или не содержит ссылок.")
        return

    logger.info(f"Найдено {len(urls)} уникальных ссылок для полной очистки.")
    if JOIN_AND_LEAVE_PRIVATE_CHATS:
        logger.info("Режим авто-входа в приватные чаты ВКЛЮЧЕН.")
    else:
        logger.info("Режим авто-входа в приватные чаты ВЫКЛЮЧЕН.")

    confirm = input(
        f"Вы уверены, что хотите удалить ВСЕ свои сообщения из {len(urls)} чатов? (да/нет): "
    ).lower()
    if confirm != "да":
        logger.info("Операция отменена.")
        return

    for i, url in enumerate(urls, 1):
        logger.info(f"\n--- [{i}/{len(urls)}] Обработка ссылки: {url} ---")
        result = await get_chat_entity_from_link(url, client)
        if not result:
            logger.error("Не удалось получить доступ к чату по ссылке. Пропускаю.")
            stats["chats_failed"] += 1
            continue

        initial_chat, did_join = result
        await process_chat_for_link_cleaning(client, initial_chat)
        stats["chats_processed"] += 1

        if did_join:
            logger.info(
                f"Обработка завершена. Выхожу из чата '{initial_chat.title}'..."
            )
            try:
                await client.leave_chat(initial_chat.id)
                logger.info("Успешно покинул чат.")
            except Exception as leave_error:
                logger.error(
                    f"Не удалось покинуть чат '{initial_chat.title}': {leave_error}"
                )
        await asyncio.sleep(1)


def clean_json_id(json_id) -> Optional[int]:
    try:
        if isinstance(json_id, str):
            cleaned = "".join(c for c in json_id if c.isdigit() or c == "-")
            return int(cleaned)
        return int(json_id)
    except (ValueError, TypeError):
        return None


def get_possible_telegram_ids(json_id_val, chat_type: str) -> List[int]:
    cleaned_id = clean_json_id(json_id_val)
    if cleaned_id is None:
        return []
    if cleaned_id < 0:
        return [cleaned_id]
    raw_id = cleaned_id
    if chat_type in ("personal_chat", "bot_chat"):
        return [raw_id]
    elif chat_type in ("private_group", "public_group"):
        return [int(f"-100{raw_id}"), -raw_id, raw_id]
    elif chat_type in ("private_channel", "public_channel"):
        return [int(f"-100{raw_id}")]
    else:
        return [raw_id, -raw_id, int(f"-100{raw_id}")]


def extract_text_from_json_msg(msg: dict) -> str:
    text_val = msg.get("text", "")
    if isinstance(text_val, list):
        parts = []
        for part in text_val:
            if isinstance(part, dict):
                parts.append(part.get("text", ""))
            else:
                parts.append(str(part))
        return "".join(parts)
    return str(text_val)


def parse_export_json(filepath: str, me_id: int) -> List[dict]:
    if not os.path.exists(filepath):
        logger.error(f"Файл {filepath} не найден!")
        return []

    logger.info(
        "Загрузка JSON-файла в память. Это может занять некоторое время для больших архивов..."
    )
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Ошибка при чтении или парсинге JSON: {e}")
        return []

    uid = data.get("personal_information", {}).get("user_id")
    if uid:
        my_id = int(uid)
        logger.info(f"Определен ваш Telegram ID из архива: {my_id}")
    else:
        my_id = me_id
        logger.info(
            f"Личный ID в архиве не найден. Используется ваш ID из сессии: {my_id}"
        )

    my_id_str = str(my_id)
    my_id_user_str = f"user{my_id}"

    chats_list = []
    raw_chats = []

    # 1. Извлекаем активные чаты из выгрузки
    if "chats" in data and isinstance(data["chats"], dict) and "list" in data["chats"]:
        for chat_obj in data["chats"]["list"]:
            chat_obj["is_left"] = False
            raw_chats.append(chat_obj)

    # 2. Извлекаем покинутые/забаненные чаты (раздел left_chats)
    if (
        "left_chats" in data
        and isinstance(data["left_chats"], dict)
        and "list" in data["left_chats"]
    ):
        for chat_obj in data["left_chats"]["list"]:
            chat_obj["is_left"] = True
            raw_chats.append(chat_obj)

    # 3. Поддержка одиночного экспорта чата
    if not raw_chats and "messages" in data and "id" in data:
        data["is_left"] = False
        raw_chats = [data]

    if not raw_chats:
        logger.error(
            "Неверный формат JSON архива. Убедитесь, что это файл result.json из Telegram Desktop."
        )
        return []

    for chat in raw_chats:
        chat_id = chat.get("id")
        chat_name = chat.get("name") or f"ID {chat_id}"
        chat_type = chat.get("type") or "unknown"
        chat_username = chat.get("username")  # Сохраняем юзернейм для авто-входа
        messages = chat.get("messages", [])

        own_msg_ids = []
        own_raw_messages = []
        for msg in messages:
            if msg.get("type") != "message":
                continue

            from_id = msg.get("from_id")
            from_id_str = None
            if isinstance(from_id, dict):
                for key in ["user_id", "channel_id", "chat_id"]:
                    if key in from_id:
                        from_id_str = str(from_id[key])
                        break
            elif from_id is not None:
                from_id_str = str(from_id)

            if from_id_str:
                if from_id_str == my_id_str or from_id_str == my_id_user_str:
                    msg_id = msg.get("id")
                    if msg_id:
                        own_msg_ids.append(int(msg_id))
                        own_raw_messages.append(msg)

        if own_msg_ids:
            chats_list.append(
                {
                    "id": chat_id,
                    "name": chat_name,
                    "type": chat_type,
                    "username": chat_username,
                    "is_left": chat.get("is_left", False),
                    "own_messages": own_msg_ids,
                    "_raw_messages": own_raw_messages,
                }
            )

    return chats_list


def parse_selection_string(sel_str: str, max_val: int) -> Set[int]:
    indices = set()
    parts = sel_str.replace(" ", "").split(",")
    for part in parts:
        if not part:
            continue
        if "-" in part:
            try:
                start_str, end_str = part.split("-", 1)
                start = int(start_str) - 1
                end = int(end_str) - 1
                if 0 <= start <= end < max_val:
                    for i in range(start, end + 1):
                        indices.add(i)
            except ValueError:
                pass
        else:
            try:
                val = int(part) - 1
                if 0 <= val < max_val:
                    indices.add(val)
            except ValueError:
                pass
    return indices


def resolve_chat_from_cache(chat: dict, dialogs_cache: dict) -> Optional[types.Chat]:
    json_id = chat["id"]
    if json_id in dialogs_cache:
        return dialogs_cache[json_id]
    chat_type = chat["type"]
    possible_ids = get_possible_telegram_ids(json_id, chat_type)
    for pid in possible_ids:
        if pid in dialogs_cache:
            return dialogs_cache[pid]
    return None


async def resolve_chat_on_demand(
    client: Client, chat: dict, dialogs_cache: dict
) -> Optional[types.Chat]:
    cached = resolve_chat_from_cache(chat, dialogs_cache)
    if cached:
        return cached

    json_id = chat["id"]
    chat_type = chat["type"]
    possible_ids = get_possible_telegram_ids(json_id, chat_type)

    # 1. Если у чата есть сохраненный username в JSON, пробуем сначала получить чат по нему
    username = chat.get("username")
    if username:
        try:
            resolved = await client.get_chat(username)
            if resolved:
                dialogs_cache[json_id] = resolved
                dialogs_cache[resolved.id] = resolved
                chat["resolved_id"] = resolved.id
                return resolved
        except Exception:
            pass

    # 2. Если по username не вышло или его нет, пробуем числовые ID
    for pid in possible_ids:
        try:
            resolved = await client.get_chat(pid)
            if resolved:
                dialogs_cache[json_id] = resolved
                dialogs_cache[resolved.id] = resolved
                chat["resolved_id"] = resolved.id
                return resolved
        except Exception:
            continue

    return None


def print_chats_table(chats_list: List[dict], dialogs_cache: dict):
    print("\nТекущий список чатов:")
    print(
        f"{'№':<5} | {'Название чата':<35} | {'Тип':<15} | {'Сообщений':<10} | {'Доступность в аккаунте'}"
    )
    print("-" * 95)
    for idx, chat in enumerate(chats_list, 1):
        resolved = resolve_chat_from_cache(chat, dialogs_cache)

        # Помечаем покинутые чаты префиксом [Left] в колонке типа
        chat_type = chat["type"]
        display_type = f"[Left] {chat_type}" if chat.get("is_left") else chat_type

        if resolved:
            status = f"Доступен (ID: {resolved.id})"
            chat["resolved_id"] = resolved.id
        else:
            resolved_id_stored = chat.get("resolved_id")
            if resolved_id_stored and resolved_id_stored != chat["id"]:
                status = f"Доступен (ID: {resolved_id_stored})"
            else:
                if chat.get("is_left"):
                    if chat.get("username"):
                        status = "Покинут (публичный, авто-вход при очистке)"
                    else:
                        status = "Покинут (приватный, требуется инвайт-ссылку)"
                else:
                    status = "Не проверен (проверка при очистке)"

                possible_ids = get_possible_telegram_ids(chat["id"], chat["type"])
                chat["resolved_id"] = possible_ids[0] if possible_ids else chat["id"]

        print(
            f"{idx:<5} | {chat['name'][:33]:<35} | {display_type[:15]:<15} | {len(chat['own_messages']):<10} | {status}"
        )


def show_chat_details(chat: dict, resolved_chat: Optional[types.Chat]):
    print("\n" + "=" * 50)
    print("                ДЕТАЛИ ЧАТА ИЗ АРХИВА")
    print("=" * 50)
    print(f"Название в архиве:  {chat['name']}")
    print(f"ID в архиве:        {chat['id']}")
    print(f"Тип в архиве:       {chat['type']}")
    print(f"Всего сообщений:    {len(chat['own_messages'])}")

    if resolved_chat:
        print("Статус в аккаунте:  Доступен")
        print(f"  - Реальный ID:    {resolved_chat.id}")
        print(f"  - Заголовок:      {resolved_chat.title or resolved_chat.first_name}")
        if resolved_chat.username:
            print(f"  - Юзернейм:       @{resolved_chat.username}")
    else:
        print("Статус в аккаунте:  Не сопоставлен (не найден в ваших диалогах)")

    messages = chat.get("_raw_messages", [])
    if messages:
        first_msg = messages[0]
        last_msg = messages[-1]
        print(
            f"Период переписки:   с {first_msg.get('date', 'N/A')} по {last_msg.get('date', 'N/A')}"
        )

        print("\nПоследние 5 ваших сообщений:")
        for m in messages[-5:]:
            date_str = m.get("date", "N/A").replace("T", " ")
            text = extract_text_from_json_msg(m)
            if len(text) > 70:
                text = text[:67] + "..."
            print(f"  [{date_str}] ID {m.get('id')}: {text}")
    print("=" * 50 + "\n")


async def delete_messages_with_fallback(
    client: Client,
    chat_id: Union[int, str],
    chat_name_for_log: str,
    message_ids: List[int],
    revoke: bool,
) -> bool:
    success = await delete_batch_own_messages(
        client, chat_id, chat_name_for_log, message_ids, revoke
    )
    if success:
        return True

    logger.warning(
        f"Пакетное удаление {len(message_ids)} сообщений в '{chat_name_for_log}' отклонено. Включаем поштучный режим-фоллбек..."
    )
    success_count = 0
    for mid in message_ids:
        try:
            await client.delete_messages(
                chat_id=chat_id, message_ids=mid, revoke=revoke
            )
            success_count += 1
            if revoke:
                stats["deleted_for_all"] += 1
            else:
                stats["deleted_for_me"] += 1
            await asyncio.sleep(0.15)
        except errors.MessageDeleteForbidden:
            try:
                await client.delete_messages(
                    chat_id=chat_id, message_ids=mid, revoke=False
                )
                success_count += 1
                stats["deleted_for_me"] += 1
                await asyncio.sleep(0.15)
            except Exception:
                stats["failed_to_delete_own"] += 1
        except errors.FloodWait as e:
            logger.warning(f"FloodWait при поштучном удалении. Ждем {e.value + 2} сек.")
            await asyncio.sleep(e.value + 2)
            try:
                await client.delete_messages(
                    chat_id=chat_id, message_ids=mid, revoke=revoke
                )
                success_count += 1
                if revoke:
                    stats["deleted_for_all"] += 1
                else:
                    stats["deleted_for_me"] += 1
            except Exception:
                stats["failed_to_delete_own"] += 1
        except Exception:
            stats["failed_to_delete_own"] += 1

    logger.info(
        f"Резервное удаление завершено: успешно очищено {success_count} из {len(message_ids)} сообщений."
    )
    return success_count > 0


async def execute_json_cleaning(
    client: Client, selected_chats: List[dict], dialogs_cache: dict
):
    global stats
    print(f"\nВыбрано чатов для очистки: {len(selected_chats)}")
    for c in selected_chats:
        print(f"  - {c['name']} ({len(c['own_messages'])} сообщений)")

    confirm = (
        input(
            "\nЗапустить процедуру удаления? Восстановить сообщения невозможно! (да/нет): "
        )
        .strip()
        .lower()
    )
    if confirm != "да":
        logger.info("Отменено.")
        return

    deletion_mode_choice = await get_user_choice(
        "Выберите режим удаления:",
        [
            "Удалить сообщения ТОЛЬКО У СЕБЯ (быстро, безопасно)",
            "Попытаться удалить сообщения У ВСЕХ",
        ],
    )
    revoke = (
        deletion_mode_choice != "Удалить сообщения ТОЛЬКО У СЕБЯ (быстро, безопасно)"
    )

    logger.info("Запуск процесса очистки по JSON-архиву...")

    for idx, chat in enumerate(selected_chats, 1):
        chat_name = chat["name"]
        chat_id = chat.get("resolved_id") or chat["id"]
        message_ids = chat["own_messages"]

        logger.info(
            f"\n--- [{idx}/{len(selected_chats)}] Обработка чата '{chat_name}' (ID: {chat_id}) ---"
        )
        logger.info(f"Найдено {len(message_ids)} сообщений для удаления.")

        real_chat_id = None
        did_join = False
        resolved_entity = None

        # Шаг 1. Авто-вступление для покинутых публичных чатов (если сохранен username)
        if chat.get("is_left") and chat.get("username"):
            logger.info(
                f"Обнаружен покинутый публичный чат. Автоматически вступаем для очистки: @{chat['username']}"
            )
            try:
                joined = await client.join_chat(chat["username"])
                if joined:
                    logger.info(f"Успешно вошли в чат '{joined.title}'!")
                    real_chat_id = joined.id
                    resolved_entity = joined
                    did_join = True
            except Exception as join_e:
                logger.warning(
                    f"Не удалось автоматически вступить в чат по юзернейму @{chat['username']}: {join_e}"
                )

        # Шаг 2. Обычное разрешение по кэшу / API (если не вступили автоматически на шаге 1)
        if not real_chat_id:
            resolved_entity = await resolve_chat_on_demand(client, chat, dialogs_cache)
            if resolved_entity:
                real_chat_id = getattr(resolved_entity, "id", None)
                if real_chat_id:
                    logger.info(
                        f"Связь с чатом установлена: '{getattr(resolved_entity, 'title', '') or getattr(resolved_entity, 'first_name', '')}'"
                    )

        # Шаг 3. Запасной фоллбек на ручной резолв, если связь по-прежнему не установлена
        if not real_chat_id:
            try:
                resolved_entity = await client.get_chat(chat_id)
                if resolved_entity:
                    real_chat_id = getattr(resolved_entity, "id", None)
                    if real_chat_id:
                        logger.info(
                            f"Связь с чатом установлена напрямую по ID: '{getattr(resolved_entity, 'title', '') or getattr(resolved_entity, 'first_name', '')}'"
                        )
            except Exception as e:
                logger.warning(
                    f"Не удалось получить доступ напрямую к ID {chat_id} ({type(e).__name__}). Пробуем альтернативные ID..."
                )

                alt_ids = get_possible_telegram_ids(chat["id"], chat["type"])
                success_resolve = False
                for aid in alt_ids:
                    if aid == chat_id:
                        continue
                    try:
                        resolved_entity = await client.get_chat(aid)
                        if resolved_entity:
                            real_chat_id = getattr(resolved_entity, "id", None)
                            if real_chat_id:
                                logger.info(
                                    f"Альтернативный ID {aid} подошел! Чат: '{getattr(resolved_entity, 'title', '') or getattr(resolved_entity, 'first_name', '')}'"
                                )
                                success_resolve = True
                                break
                    except Exception:
                        continue

                if not success_resolve:
                    # Фоллбек уровня 2: Запрос реквизитов вручную
                    print(
                        f"\n[!] Чат '{chat_name}' (ID в архиве: {chat['id']}) недоступен через API."
                    )
                    print(
                        "Возможные причины: вы вышли из чата, это приватная группа или чат был удален."
                    )
                    print("Варианты действий:")
                    print(
                        "  1. Ввести юзернейм чата (@username) или инвайт-ссылку (https://t.me/...) вручную"
                    )
                    print("  2. Пропустить этот чат")
                    fallback_choice = input(
                        "Выберите вариант (1-2, по умолчанию 2): "
                    ).strip()
                    if fallback_choice == "1":
                        user_link = input("Введите ссылку или юзернейм: ").strip()
                        if user_link:
                            res = await get_chat_entity_from_link(user_link, client)
                            if res:
                                resolved_entity, did_join_fallback = res
                                real_chat_id = getattr(resolved_entity, "id", None)
                                if real_chat_id:
                                    logger.info(
                                        f"Фоллбек успешно сработал! Найден чат: '{getattr(resolved_entity, 'title', '') or getattr(resolved_entity, 'first_name', '')}'"
                                    )
                                    chat["did_join_via_fallback"] = did_join_fallback
                                else:
                                    logger.error(
                                        "У разрешенного по ссылке чата отсутствует ID."
                                    )
                                    stats["chats_failed"] += 1
                                    continue
                            else:
                                logger.error("Не удалось разрешить указанную ссылку.")
                                stats["chats_failed"] += 1
                                continue
                        else:
                            stats["chats_failed"] += 1
                            continue
                    else:
                        logger.info("Пропуск чата.")
                        stats["chats_failed"] += 1
                        continue

        if not real_chat_id:
            logger.error(f"Не удалось получить ID для чата '{chat_name}'. Пропускаем.")
            stats["chats_failed"] += 1
            continue

        stats["chats_processed"] += 1
        stats["total_found_own"] += len(message_ids)

        # Выполнение удаления
        chunks = [message_ids[i : i + 100] for i in range(0, len(message_ids), 100)]
        for ch_idx, chunk in enumerate(chunks, 1):
            if len(chunks) > 1:
                logger.info(
                    f"Удаление порции {ch_idx}/{len(chunks)} ({len(chunk)} шт.) в чате '{chat_name}'..."
                )

            success = await delete_messages_with_fallback(
                client, real_chat_id, chat_name, chunk, revoke=revoke
            )
            if not success:
                logger.warning(f"Порция {ch_idx} не была удалена.")

            await asyncio.sleep(1.5)

        # Шаг 4. Если мы автоматически или вручную вступали в чат для очистки, покидаем его
        if (did_join or chat.get("did_join_via_fallback")) and real_chat_id:
            logger.info(
                f"Выходим из чата '{getattr(resolved_entity, 'title', 'N/A')}'..."
            )
            try:
                await client.leave_chat(real_chat_id)
                logger.info("Успешно покинули чат.")
            except Exception as leave_error:
                logger.error(f"Не удалось покинуть чат: {leave_error}")


async def iter_dialogs_safe(client: Client, limit: int = 0):
    current = 0
    total = limit or (1 << 31) - 1
    chunk_limit = min(100, total)
    offset_date = 0
    offset_id = 0
    offset_peer = raw.types.InputPeerEmpty()

    while True:
        try:
            r = await client.invoke(
                raw.functions.messages.GetDialogs(
                    offset_date=offset_date,
                    offset_id=offset_id,
                    offset_peer=offset_peer,
                    limit=chunk_limit,
                    hash=0,
                ),
                sleep_threshold=60,
            )
        except Exception as e:
            logger.debug(f"Не удалось получить диалоги через Raw API: {e}")
            return

        users = {i.id: i for i in r.users}
        chats = {i.id: i for i in r.chats}
        messages = {}

        for message in r.messages:
            if isinstance(message, raw.types.MessageEmpty):
                continue
            chat_id = utils.get_peer_id(message.peer_id)
            try:
                messages[chat_id] = await types.Message._parse(
                    client, message, users, chats
                )
            except Exception:
                continue

        dialogs = []
        for dialog in r.dialogs:
            if not isinstance(dialog, raw.types.Dialog):
                continue
            try:
                parsed = types.Dialog._parse(client, dialog, messages, users, chats)
                dialogs.append(parsed)
            except Exception:
                continue

        if not dialogs:
            return

        for dialog in dialogs:
            yield dialog
            current += 1
            if current >= total:
                return

        last = dialogs[-1]

        top_msg = getattr(last, "top_message", None)
        if top_msg:
            offset_id = top_msg.id
            offset_date = utils.datetime_to_timestamp(top_msg.date)
        else:
            found_offset = False
            for prev_dialog in reversed(dialogs[:-1]):
                prev_top = getattr(prev_dialog, "top_message", None)
                if prev_top:
                    offset_id = prev_top.id
                    offset_date = utils.datetime_to_timestamp(prev_top.date)
                    found_offset = True
                    break
            if not found_offset:
                offset_id = 0
                import time

                offset_date = int(time.time())

        try:
            offset_peer = await client.resolve_peer(last.chat.id)
        except Exception:
            return


async def run_json_based_cleaning(client: Client):
    global stats
    me = await client.get_me()
    me_id = getattr(me, "id", 0) if me else 0

    json_path = input(
        "Введите путь к файлу result.json (или нажмите Enter для './result.json'): "
    ).strip()
    if not json_path:
        json_path = "result.json"

    if not os.path.exists(json_path):
        logger.error(f"Файл '{json_path}' не найден. Проверьте правильность пути.")
        return

    parsed_chats = parse_export_json(json_path, me_id)
    if not parsed_chats:
        logger.warning("В указанном JSON-файле не найдено чатов с вашими сообщениями.")
        return

    parsed_chats.sort(key=lambda x: len(x["own_messages"]), reverse=True)

    dialogs_cache = {}

    filtered_chats = parsed_chats.copy()

    while True:
        print("\n" + "=" * 60)
        print("          МЕНЮ ДЕТАЛЬНОГО АНАЛИЗА JSON-АРХИВА")
        print("=" * 60)
        print("  1. Показать текущий список чатов (таблица)")
        print("  2. Фильтрация/Поиск чатов (по названию или типу)")
        print("  3. Детальный осмотр чата (с просмотром примеров сообщений)")
        print("  4. Перейти к зачистке чатов")
        print("  5. Сбросить фильтры поиска")
        print("  6. Вернуться в главное меню")
        print("=" * 60)

        menu_choice = input("Выберите действие (1-6): ").strip()

        if menu_choice == "1":
            print_chats_table(filtered_chats, dialogs_cache)

        elif menu_choice == "2":
            print("\nВарианты фильтрации:")
            print("  1. Поиск по тексту в названии")
            print("  2. Фильтр по типу чата")
            filter_type = input("Выберите тип фильтра (1-2): ").strip()
            if filter_type == "1":
                keyword = (
                    input("Введите слово для поиска в названиях: ").strip().lower()
                )
                filtered_chats = [
                    c for c in parsed_chats if keyword in c["name"].lower()
                ]
                logger.info(f"Отфильтровано: найдено {len(filtered_chats)} чатов.")
            elif filter_type == "2":
                print(
                    "Типы: personal_chat, private_group, public_group, private_channel, public_channel"
                )
                t_keyword = input("Введите тип чата для фильтрации: ").strip().lower()
                filtered_chats = [
                    c for c in parsed_chats if t_keyword in c["type"].lower()
                ]
                logger.info(f"Отфильтровано: найдено {len(filtered_chats)} чатов.")

        elif menu_choice == "3":
            if not filtered_chats:
                logger.warning("Список пуст.")
                continue
            print_chats_table(filtered_chats, dialogs_cache)
            try:
                num_str = input(
                    f"Введите номер чата для осмотра (1-{len(filtered_chats)}): "
                ).strip()
                chat_idx = int(num_str) - 1
                if 0 <= chat_idx < len(filtered_chats):
                    target_chat = filtered_chats[chat_idx]

                    resolved = await resolve_chat_on_demand(
                        client, target_chat, dialogs_cache
                    )
                    show_chat_details(target_chat, resolved)
                else:
                    print("Неверный номер.")
            except ValueError:
                print("Неверный ввод.")

        elif menu_choice == "4":
            if not filtered_chats:
                logger.warning("Список чатов для очистки пуст.")
                continue
            print_chats_table(filtered_chats, dialogs_cache)
            print(
                "\nВведите номера чатов для зачистки (например, '1, 3, 5-8' или 'all'):"
            )
            selection_input = input("Выбор: ").strip().lower()
            if not selection_input or selection_input == "exit":
                continue

            if selection_input == "all":
                selected_indices = set(range(len(filtered_chats)))
            else:
                selected_indices = parse_selection_string(
                    selection_input, len(filtered_chats)
                )

            if not selected_indices:
                logger.warning("Чаты не выбраны.")
                continue

            selected_chats = [filtered_chats[i] for i in sorted(selected_indices)]
            await execute_json_cleaning(client, selected_chats, dialogs_cache)
            break

        elif menu_choice == "5":
            filtered_chats = parsed_chats.copy()
            logger.info("Фильтры сброшены. Показываются все чаты.")

        elif menu_choice == "6":
            logger.info("Возврат в главное меню.")
            return


async def main() -> None:
    load_dotenv()
    logger.info("Запуск TCleaner...")
    api_id_str = (
        os.environ.get("TELEGRAM_API_ID") or input("Введите ваш API ID: ").strip()
    )
    api_hash = (
        os.environ.get("TELEGRAM_API_HASH") or input("Введите ваш API Hash: ").strip()
    )
    phone_number = os.environ.get("TELEGRAM_PHONE_NUMBER")
    try:
        api_id = int(api_id_str)
    except (ValueError, TypeError):
        logger.critical("API ID должен быть числом.")
        return

    async with Client(
        SESSION_NAME,
        api_id=api_id,
        api_hash=api_hash,
        phone_number=phone_number or None,
    ) as client:
        me = await client.get_me()
        logger.info(f"Успешный вход как {me.first_name} (@{me.username or 'N/A'}).")

        try:
            main_choice = await get_user_choice(
                "Выберите режим работы:",
                [
                    f"Очистка по ключевым словам (из {CONFIG_FILE_KEYWORDS})",
                    f"Полная очистка чатов по ссылкам (из {CONFIG_FILE_LINKS})",
                    "Очистка на основе JSON-архива выгрузки Telegram (result.json)",
                ],
            )
            if "ключевым словам" in main_choice:
                await run_keyword_cleaning(client)
            elif "по ссылкам" in main_choice:
                await run_link_based_cleaning(client)
            elif "JSON-архива" in main_choice:
                await run_json_based_cleaning(client)
        except (KeyboardInterrupt, EOFError, Exception) as e:
            if not isinstance(e, (KeyboardInterrupt, EOFError)):
                logger.error(f"Произошла ошибка во время настройки: {e}")
            return

    logger.info("\n" + "=" * 50)
    logger.info("         Процесс ЗАВЕРШЕН")
    logger.info("=" * 50)
    logger.info(f"Обработано чатов/ссылок: {stats['chats_processed']}")
    if stats["chats_failed"] > 0:
        logger.warning(f"Чатов/ссылок завершено с ошибками: {stats['chats_failed']}")
    logger.info(f"Проверено сообщений (API поиск): {stats['total_checked_api']}")
    logger.info(f"Найдено СВОИХ сообщений: {stats['total_found_own']}")
    logger.info(f"Успешно удалено СВОИХ У ВСЕХ: {stats['deleted_for_all']}")
    logger.info(f"Успешно удалено СВОИХ ТОЛЬКО У СЕБЯ: {stats['deleted_for_me']}")
    if stats["failed_to_delete_own"] > 0:
        logger.error(f"Не удалось удалить СВОИХ: {stats['failed_to_delete_own']}")
    logger.info("=" * 50)


if __name__ == "__main__":
    loop = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
    except (KeyboardInterrupt, EOFError):
        logger.info("\nСкрипт прерван пользователем.")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        if loop and not loop.is_closed():
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())

                pending = asyncio.all_tasks(loop)
                if pending:
                    for task in pending:
                        task.cancel()
                    loop.run_until_complete(asyncio.wait(pending, timeout=2.0))
            except Exception:
                pass
            finally:
                try:
                    loop.close()
                except Exception:
                    pass
        if _COLORAMA_AVAILABLE:
            print(colorama.Style.RESET_ALL)
