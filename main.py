import asyncio
import logging
import os
import re
import sys
from functools import wraps
from typing import List, Optional, Set, Tuple, Union

from dotenv import load_dotenv
from pyrogram import Client, errors, types
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
    "total_checked_api": 0, "total_checked_manual": 0, "total_found_own": 0,
    "total_found_other": 0, "deleted_for_me": 0, "deleted_for_all": 0,
    "failed_to_delete_own": 0, "failed_revoke_but_deleted_for_me": 0,
    "attempted_delete_other": 0, "failed_to_delete_other": 0,
    "chats_processed": 0, "chats_failed": 0, "dialogs_found": 0,
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
                    logger.warning(f"Слишком много запросов. Ждем {wait_time} секунд...")
                    await asyncio.sleep(wait_time)
                    return await func(*args, **kwargs)
                except RETRYABLE_EXCEPTIONS as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Ошибка сети/сервера ({type(e).__name__}). Попытка {attempt + 1}/{max_retries}. Повтор через {delay} сек...")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Превышено количество попыток. Ошибка: {e}")
                        return None
                except Exception as e:
                    logger.error(f"В функции '{func.__name__}' произошла неисправимая ошибка: {type(e).__name__}: {e}")
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
    choice = await get_user_choice("Выберите цель для поиска по ключевым словам:", ["Удалить из ВСЕХ личных чатов и бесед", "Удалить из КОНКРЕТНОГО чата"])
    if choice == "Удалить из ВСЕХ личных чатов и бесед":
        return None
    elif choice == "Удалить из КОНКРЕТНОГО чата":
        while True:
            chat_input = input("Введите Имя чата, @username, Номер телефона или ID чата: ").strip()
            if not chat_input: continue
            try:
                entity: types.Chat = await client.get_chat(chat_input)
                entity_name = entity.title or entity.username or f"ID: {entity.id}"
                logger.info(f"Найден чат: {entity_name} (ID: {entity.id}, Тип: {entity.type.name if entity.type else 'N/A'})")
                confirm = input(f"Это верный чат? (да/нет): ").lower()
                if confirm == "да": return entity
            except Exception as e:
                logger.error(f"Чат '{chat_input}' не найден или произошла ошибка: {e}")
            retry = input("Попробовать найти другой чат? (да/нет): ").lower()
            if retry != "да": raise Exception("Пользователь отменил выбор чата.")
    return None

async def delete_batch_own_messages(client: Client, chat_id: Union[int, str], chat_name_for_log: str, message_ids: List[int], revoke: bool) -> bool:
    global stats
    if not message_ids: return True
    action = "у всех" if revoke else "только у себя"
    count = len(message_ids)
    try:
        await client.delete_messages(chat_id=chat_id, message_ids=message_ids, revoke=revoke)
        logger.info(f"УСПЕШНО: Удалено {count} ВАШИХ сообщений {action} в '{chat_name_for_log}'")
        if revoke: stats["deleted_for_all"] += count
        else: stats["deleted_for_me"] += count
        return True
    except errors.MessageDeleteForbidden:
        logger.warning(f"ЗАПРЕЩЕНО удалять {count} ВАШИХ сообщений {action} в '{chat_name_for_log}'.")
        if revoke:
            logger.info(f"Попытка удалить те же сообщения только у себя.")
            success_for_me = await delete_batch_own_messages(client, chat_id, chat_name_for_log, message_ids, revoke=False)
            if success_for_me: stats["failed_revoke_but_deleted_for_me"] += count
            else: stats["failed_to_delete_own"] += count
            return success_for_me
        else:
            stats["failed_to_delete_own"] += count
            return False
    except errors.FloodWait as e:
        wait_time = e.value + 5
        logger.warning(f"[FloodWait] при удалении. Ожидание {wait_time} секунд.")
        await asyncio.sleep(wait_time)
        return await delete_batch_own_messages(client, chat_id, chat_name_for_log, message_ids, revoke)
    except Exception as e:
        logger.error(f"НЕИЗВЕСТНАЯ ОШИБКА при удалении ВАШИХ сообщений: {e}")
        stats["failed_to_delete_own"] += count
        return False

async def attempt_delete_other_message(client: Client, chat_id: Union[int, str], chat_name_for_log: str, message_id: int):
    global stats
    stats["attempted_delete_other"] += 1
    try:
        await client.delete_messages(chat_id=chat_id, message_ids=message_id, revoke=True)
        logger.info(f"УСПЕХ: Удалось удалить ЧУЖОЕ сообщение ID {message_id} у всех в '{chat_name_for_log}'.")
    except (errors.MessageDeleteForbidden, errors.RpcCallFail, errors.MessageAuthorRequired):
        stats["failed_to_delete_other"] += 1
    except Exception:
        stats["failed_to_delete_other"] += 1


URL_PATTERN_FOR_EXTRACTION = re.compile(r"https://t\.me/(?:[a-zA-Z0-9_]+|c/\d+|joinchat/[-_a-zA-Z0-9]+|\+[-_a-zA-Z0-9]+)(?:/\d+)?")
URL_PATTERN_FOR_PARSING = re.compile(r"https://t\.me/(?:(c/\d+)|(\+[-_a-zA-Z0-9]+|joinchat/[-_a-zA-Z0-9]+)|([a-zA-Z0-9_]+))(?:/(\d+))?")

def extract_urls_from_file(filename: str) -> List[str]:
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
            urls = {match.group(0) for match in URL_PATTERN_FOR_EXTRACTION.finditer(content)}
            return list(urls)
    except FileNotFoundError:
        logger.error(f"Файл со ссылками '{filename}' не найден.")
        return []

@retry_on_exception(max_retries=3, delay=5)
async def get_chat_entity_from_link(url: str, app: Client) -> Optional[Tuple[types.Chat, bool]]:
    match = URL_PATTERN_FOR_PARSING.match(url)
    if not match: return None
    private_chat_part, invite_hash, username, _ = match.groups()
    chat_identifier = username or invite_hash or (int("-100" + private_chat_part.split('/')[1]) if private_chat_part else url)
    try:
        chat = await app.get_chat(chat_identifier)
        return chat, False
    except (errors.InviteHashInvalid, errors.InviteHashExpired, errors.UserNotParticipant) as e:
        if JOIN_AND_LEAVE_PRIVATE_CHATS and invite_hash:
            logger.info(f"Попытка входа в приватный чат: {url}")
            try:
                joined_chat = await app.join_chat(invite_hash)
                logger.info(f"Успешно вошел в чат: '{joined_chat.title}'")
                return joined_chat, True
            except Exception as join_error:
                logger.error(f"Не удалось войти в чат по ссылке {url}. Ошибка: {join_error}")
                return None
        else:
            logger.warning(f"Ссылка недействительна или вы не участник: {url} ({type(e).__name__})")
            return None
    except (errors.UsernameInvalid, errors.UsernameNotOccupied, errors.ChannelInvalid, errors.PeerIdInvalid):
        logger.warning(f"Ссылка недействительна, устарела или недоступна: {url}")
        return None
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при получении чата '{chat_identifier}': {e}")
        raise

@retry_on_exception(max_retries=3, delay=5)
async def process_chat_for_link_cleaning(app: Client, initial_chat: types.Chat):
    target_chat: Optional[types.Chat] = None
    if initial_chat.linked_chat:
        target_chat = initial_chat.linked_chat
        logger.info(f"Найден канал '{initial_chat.title}'. Перехожу в связанный чат: '{target_chat.title}'")
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
        messages_to_delete = [msg.id async for msg in app.search_messages(target_chat.id, from_user="me")]
        if not messages_to_delete:
            logger.info("Моих сообщений в этом чате не найдено.")
            return
        logger.info(f"Найдено {len(messages_to_delete)} ваших сообщений для удаления.")
        for i in range(0, len(messages_to_delete), 100):
            chunk = messages_to_delete[i:i + 100]
            await delete_batch_own_messages(app, target_chat.id, target_chat.title, chunk, revoke=True)
            if len(messages_to_delete) > 100: await asyncio.sleep(1)
    except (errors.UserNotParticipant, errors.ChannelPrivate):
        logger.warning(f"Не могу искать сообщения в '{target_chat.title}'. Возможно, вы не участник.")
    except Exception as e:
        logger.error(f"Ошибка при удалении сообщений из '{target_chat.title}': {e}")
        raise


async def run_keyword_cleaning(client: Client):
    global stats
    keywords_config = load_keywords(CONFIG_FILE_KEYWORDS)
    if not keywords_config: return
    
    target_chat_obj = await get_target_chat_for_keywords(client)
    deletion_mode_choice = await get_user_choice("Выберите режим удаления:", ["Удалить сообщения ТОЛЬКО У СЕБЯ", "Попытаться удалить сообщения У ВСЕХ"])
    delete_for_everyone = deletion_mode_choice == "Попытаться удалить сообщения У ВСЕХ"

    dialogs_to_process: List[Tuple[Union[int, str], str, ChatType]] = []
    if target_chat_obj:
        dialogs_to_process.append((target_chat_obj.id, target_chat_obj.title or target_chat_obj.username, target_chat_obj.type))
    else:
        logger.info("Получение списка всех диалогов...")
        async for dialog in client.get_dialogs():
            stats["dialogs_found"] += 1
            chat = dialog.chat
            if chat.type in {ChatType.PRIVATE, ChatType.GROUP, ChatType.SUPERGROUP}:
                dialogs_to_process.append((chat.id, chat.title or chat.username, chat.type))
            else:
                stats["dialogs_skipped_type"] += 1
    
    logger.info(f"Начинается обработка {len(dialogs_to_process)} диалогов по ключевым словам...")
    my_id = (await client.get_me()).id

    for i, (chat_id, chat_name, chat_type) in enumerate(dialogs_to_process, 1):
        stats["chats_processed"] += 1
        logger.info(f"\n--- Обработка чата {i}/{len(dialogs_to_process)}: '{chat_name}' ---")
        own_ids_to_delete: Set[int] = set()
        
        try:
            for keyword in keywords_config:
                async for message in client.search_messages(chat_id, query=keyword):
                    stats["total_checked_api"] += 1
                    if not message.from_user: continue
                    if message.from_user.id == my_id:
                        own_ids_to_delete.add(message.id)
                    elif delete_for_everyone and chat_type == ChatType.PRIVATE:
                        await attempt_delete_other_message(client, chat_id, chat_name, message.id)
                
                if own_ids_to_delete:
                    logger.info(f"Найдено {len(own_ids_to_delete)} ВАШИХ сообщений. Начинаю удаление...")
                    await delete_batch_own_messages(client, chat_id, chat_name, sorted(list(own_ids_to_delete)), delete_for_everyone)
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

    confirm = input(f"Вы уверены, что хотите удалить ВСЕ свои сообщения из {len(urls)} чатов? (да/нет): ").lower()
    if confirm != 'да':
        logger.info("Операция отменена.")
        return

    for i, url in enumerate(urls, 1):
        logger.info(f"\n--- [{i}/{len(urls)}] Обработка ссылки: {url} ---")
        result = await get_chat_entity_from_link(url, client)
        if not result:
            logger.error(f"Не удалось получить доступ к чату по ссылке. Пропускаю.")
            stats["chats_failed"] += 1
            continue
        
        initial_chat, did_join = result
        await process_chat_for_link_cleaning(client, initial_chat)
        stats["chats_processed"] += 1

        if did_join:
            logger.info(f"Обработка завершена. Выхожу из чата '{initial_chat.title}'...")
            try:
                await client.leave_chat(initial_chat.id)
                logger.info("Успешно покинул чат.")
            except Exception as leave_error:
                logger.error(f"Не удалось покинуть чат '{initial_chat.title}': {leave_error}")
        await asyncio.sleep(1)


async def main() -> None:
    load_dotenv()
    logger.info("Запуск TCleaner...")
    api_id_str = os.environ.get("TELEGRAM_API_ID") or input("Введите ваш API ID: ").strip()
    api_hash = os.environ.get("TELEGRAM_API_HASH") or input("Введите ваш API Hash: ").strip()
    phone_number = os.environ.get("TELEGRAM_PHONE_NUMBER")
    try:
        api_id = int(api_id_str)
    except (ValueError, TypeError):
        logger.critical("API ID должен быть числом.")
        return

    async with Client(SESSION_NAME, api_id=api_id, api_hash=api_hash, phone_number=phone_number or None) as client:
        me = await client.get_me()
        logger.info(f"Успешный вход как {me.first_name} (@{me.username or 'N/A'}).")

        try:
            main_choice = await get_user_choice(
                "Выберите режим работы:",
                [
                    f"Очистка по ключевым словам (из {CONFIG_FILE_KEYWORDS})",
                    f"Полная очистка чатов по ссылкам (из {CONFIG_FILE_LINKS})"
                ]
            )
            if "ключевым словам" in main_choice:
                await run_keyword_cleaning(client)
            elif "по ссылкам" in main_choice:
                await run_link_based_cleaning(client)
        except (KeyboardInterrupt, EOFError, Exception) as e:
            if not isinstance(e, (KeyboardInterrupt, EOFError)):
                logger.error(f"Произошла ошибка во время настройки: {e}")
            return
    
    logger.info("\n" + "=" * 50)
    logger.info("         Процесс ЗАВЕРШЕН")
    logger.info("=" * 50)
    logger.info(f"Обработано чатов/ссылок: {stats['chats_processed']}")
    if stats['chats_failed'] > 0: logger.warning(f"Чатов/ссылок завершено с ошибками: {stats['chats_failed']}")
    logger.info(f"Проверено сообщений (API поиск): {stats['total_checked_api']}")
    logger.info(f"Найдено СВОИХ сообщений: {stats['total_found_own']}")
    logger.info(f"Успешно удалено СВОИХ У ВСЕХ: {stats['deleted_for_all']}")
    logger.info(f"Успешно удалено СВОИХ ТОЛЬКО У СЕБЯ: {stats['deleted_for_me']}")
    if stats['failed_to_delete_own'] > 0: logger.error(f"Не удалось удалить СВОИХ: {stats['failed_to_delete_own']}")
    logger.info("=" * 50)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, EOFError):
        logger.info("\nСкрипт прерван пользователем.")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        if _COLORAMA_AVAILABLE:
            print(colorama.Style.RESET_ALL)