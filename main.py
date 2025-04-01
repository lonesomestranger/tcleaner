import asyncio
import logging
import os
import sys
from typing import List, Optional, Set, Tuple, Union

from dotenv import load_dotenv
from pyrogram import Client, errors, types
from pyrogram.enums import ChatType

try:
    import colorama

    colorama.init()
    _COLORAMA_AVAILABLE = True
except ImportError:
    _COLORAMA_AVAILABLE = False
    print("Библиотека Colorama не найдена. Попытка установки...")
    try:
        import subprocess

        subprocess.check_call([sys.executable, "-m", "pip", "install", "colorama"])
        import colorama

        colorama.init()
        _COLORAMA_AVAILABLE = True
        print("Colorama была установлена. Цветные логи должны появиться.")
    except Exception as e:
        print(
            f"Не удалось автоматически установить colorama: {e}. Используется базовое логирование."
        )

logger = logging.getLogger("MessageDeleter")
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

CONFIG_FILE = "config.txt"
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
                    if '"' in stripped_line:
                        logger.warning(
                            f"Ключевое слово содержит кавычки и будет обработано как точное совпадение (без поиска подстроки): {stripped_line}"
                        )
                        stripped_line = stripped_line.replace('"', "")

                    if stripped_line:
                        keywords.append(stripped_line.lower())

        log_level = logging.INFO if keywords else logging.WARNING
        logger.log(
            log_level,
            f"Загружено {len(keywords)} ключевых слов из '{filename}' для точного совпадения (API search).",
        )
        if any('"' in kw for kw in keywords):
            logger.warning(
                "Поиск подстрок (слова в кавычках) больше не поддерживается в этой версии. Все слова ищутся через API."
            )

        return keywords
    except Exception as e:
        logger.error(f"Ошибка чтения ключевых слов из '{filename}': {e}")
        return []


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


async def get_target_chat(client: Client) -> Optional[types.Chat]:
    choice = await get_user_choice(
        "Выберите цель:",
        ["Удалить из ВСЕХ личных чатов и бесед", "Удалить из КОНКРЕТНОГО чата"],
    )

    if choice == "Удалить из ВСЕХ личных чатов и бесед":
        logger.info(
            "Цель: ВСЕ личные чаты и беседы (группы/супергруппы). Каналы будут пропущены."
        )
        return None

    elif choice == "Удалить из КОНКРЕТНОГО чата":
        while True:
            chat_input = input(
                "Введите Имя чата, Имя пользователя (@username), Номер телефона или ID чата: "
            ).strip()
            if not chat_input:
                print("Ввод не может быть пустым.")
                continue
            try:
                logger.debug(f"Попытка найти чат: {chat_input}")
                entity: types.Chat = await client.get_chat(chat_input)
                entity_name = entity.title or entity.username or f"ID: {entity.id}"

                if not isinstance(entity, types.Chat) or not hasattr(entity, "id"):
                    logger.error(
                        f"Найденный объект для '{chat_input}' не является валидным чатом."
                    )
                    continue

                logger.info(
                    f"Найден чат: {entity_name} (ID: {entity.id}, Тип: {entity.type.name if entity.type else 'N/A'})"
                )
                confirm = input(
                    f"Найден '{entity_name}'. Это верный чат? (да/нет): "
                ).lower()
                if confirm == "да":
                    return entity
                else:
                    logger.info("Выбор чата отменен пользователем.")
            except (
                errors.BadRequest,
                errors.UsernameNotOccupied,
                errors.PeerIdInvalid,
            ) as e:
                logger.error(
                    f"Чат '{chat_input}' не найден или неверный идентификатор: {e}"
                )
            except errors.FloodWait as e:
                wait_time = e.value + 2
                logger.warning(
                    f"[FloodWait] Превышен лимит запросов при поиске чата. Ожидание {wait_time} секунд."
                )
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(
                    f"Неожиданная ошибка при поиске чата '{chat_input}': {type(e).__name__} - {e}"
                )
            print(
                "Не удалось найти указанный чат или произошла ошибка. Попробуйте снова или проверьте идентификатор."
            )
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

    try:
        valid_message_ids = [int(mid) for mid in message_ids]
    except (ValueError, TypeError) as e:
        logger.error(
            f"Неверные ID ВАШИХ сообщений в пакете: {message_ids}. Ошибка: {e}. Пропуск пакета."
        )
        stats["failed_to_delete_own"] += len(message_ids)
        return False

    action = "у всех" if revoke else "только у себя"
    log_chat_ref = f"'{chat_name_for_log}' (ID: {chat_id})"
    count = len(valid_message_ids)
    id_range_str = ""
    if count > 0:
        min_id = min(valid_message_ids)
        max_id = max(valid_message_ids)
        id_range_str = f" (IDs {min_id}...{max_id})" if count > 1 else f" (ID {min_id})"

    logger.info(
        f"Начинаю процесс удаления {count} ВАШИХ сообщений {action} в чате {log_chat_ref}{id_range_str}"
    )
    logger.debug(
        f"Детальный список ID ВАШИХ сообщений для удаления: {valid_message_ids}"
    )

    try:
        delete_task = asyncio.create_task(
            client.delete_messages(
                chat_id=chat_id,
                message_ids=valid_message_ids,
                revoke=revoke,
            )
        )
        await asyncio.wait_for(delete_task, timeout=60.0)

        logger.info(
            f"УСПЕШНО: Удаление завершено для {count} ВАШИХ сообщений {action} в {log_chat_ref}"
        )
        if revoke:
            stats["deleted_for_all"] += count
        else:
            stats["deleted_for_me"] += count
        return True

    except asyncio.TimeoutError:
        logger.error(
            f"ТАЙМАУТ: Операция удаления пакета ВАШИХ сообщений в {log_chat_ref} зависла и была прервана"
        )
        stats["failed_to_delete_own"] += count
        return False
    except errors.MessageDeleteForbidden:
        logger.warning(
            f"ЗАПРЕЩЕНО удалять {count} ВАШИХ сообщений {action} в чате {log_chat_ref}{id_range_str}. Возможно, сообщения слишком старые или нет прав."
        )
        if revoke:
            logger.info(
                f"Попытка удалить те же {count} ВАШИХ сообщений только у себя в чате {log_chat_ref}."
            )
            success_for_me = await delete_batch_own_messages(
                client, chat_id, chat_name_for_log, valid_message_ids, revoke=False
            )
            if success_for_me:
                stats["failed_revoke_but_deleted_for_me"] += count
            else:
                stats["failed_to_delete_own"] += count
            return success_for_me
        else:
            stats["failed_to_delete_own"] += count
            logger.error(
                f"НЕ УДАЛОСЬ удалить {count} ВАШИХ сообщений даже у себя в чате {log_chat_ref}{id_range_str}."
            )
            return False
    except errors.FloodWait as e:
        wait_time = e.value + 5
        logger.warning(
            f"[FloodWait] Превышен лимит при удалении {count} ВАШИХ сообщений в {log_chat_ref}. Ожидание {wait_time} секунд."
        )
        await asyncio.sleep(wait_time)
        logger.debug(
            f"Повторная попытка удаления пакета ВАШИХ сообщений в {log_chat_ref} после FloodWait."
        )
        return await delete_batch_own_messages(
            client, chat_id, chat_name_for_log, valid_message_ids, revoke
        )
    except errors.MessageIdsInvalid:
        logger.error(
            f"ОШИБКА: Один или несколько ID ({count} шт.) ВАШИХ сообщений в пакете для {log_chat_ref}{id_range_str} недействительны (уже удалены?). Пропуск пакета."
        )
        stats["failed_to_delete_own"] += count
        return False
    except Exception as e:
        logger.error(
            f"НЕИЗВЕСТНАЯ ОШИБКА при удалении пакета ВАШИХ сообщений ({count} шт.) {action} в {log_chat_ref}{id_range_str}: {type(e).__name__} - {e}",
            exc_info=False,
        )
        stats["failed_to_delete_own"] += count
        return False


async def attempt_delete_other_message(
    client: Client,
    chat_id: Union[int, str],
    chat_name_for_log: str,
    message_id: int,
    keyword: str,
) -> None:
    global stats
    log_chat_ref = f"'{chat_name_for_log}' (ID: {chat_id})"
    stats["attempted_delete_other"] += 1
    try:
        await client.delete_messages(
            chat_id=chat_id, message_ids=message_id, revoke=True
        )
        logger.info(
            f"УСПЕХ: Удалось удалить ЧУЖОЕ сообщение ID {message_id} у всех в {log_chat_ref}."
        )
    except (
        errors.MessageDeleteForbidden,
        errors.RpcCallFail,
        errors.MessageAuthorRequired,
    ) as e:
        logger.warning(
            f"НЕ УДАЛОСЬ (ОЖИДАЕМО): Удаление ЧУЖОГО сообщения ID {message_id} в {log_chat_ref} запрещено ({type(e).__name__})."
        )
        stats["failed_to_delete_other"] += 1
    except errors.FloodWait as e:
        wait_time = e.value + 5
        logger.warning(
            f"[FloodWait] при попытке удаления ЧУЖОГО сообщения ID {message_id} в {log_chat_ref}. Ожидание {wait_time} сек."
        )
        await asyncio.sleep(wait_time)
        logger.debug(
            f"Повторная попытка удаления ЧУЖОГО сообщения ID {message_id} в {log_chat_ref} после FloodWait."
        )
        await attempt_delete_other_message(
            client, chat_id, chat_name_for_log, message_id, keyword
        )
    except Exception as e:
        logger.error(
            f"НЕИЗВЕСТНАЯ ОШИБКА при попытке удаления ЧУЖОГО сообщения ID {message_id} в {log_chat_ref}: {type(e).__name__} - {e}",
            exc_info=False,
        )
        stats["failed_to_delete_other"] += 1


async def main() -> None:
    global stats

    load_dotenv()
    logger.info("Запуск скрипта удаления сообщений Pyrogram...")

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
        logger.critical("Введен неверный API ID. Это должно быть число.")
        return

    if not api_hash:
        logger.critical("API Hash необходим для запуска скрипта.")
        return

    keywords_config = load_keywords(CONFIG_FILE)
    if not keywords_config:
        logger.error("Ключевые слова не загружены или файл пуст. Выход.")
        return

    client = Client(
        SESSION_NAME,
        api_id=api_id,
        api_hash=api_hash,
        phone_number=phone_number or None,
    )
    logger.info("Попытка подключения к Telegram с использованием Pyrogram...")

    try:
        await client.start()
        me = await client.get_me()
        if not me:
            logger.critical("Не удалось войти. Проверьте API ключи или авторизуйтесь.")
            if client.is_connected:
                await client.stop()
            return

        my_id = me.id
        logger.info(
            f"Успешный вход как {me.first_name} (@{me.username or 'нет username'}). ID: {my_id}"
        )

        target_chat_obj: Optional[types.Chat] = None
        delete_for_everyone = False
        try:
            target_chat_obj = await get_target_chat(client)
            deletion_mode_choice = await get_user_choice(
                "Выберите режим удаления:",
                [
                    "Удалить сообщения ТОЛЬКО У СЕБЯ",
                    "Попытаться удалить сообщения У ВСЕХ (включая чужие в ЛС, если возможно)",
                ],
            )
            delete_for_everyone = (
                deletion_mode_choice
                == "Попытаться удалить сообщения У ВСЕХ (включая чужие в ЛС, если возможно)"
            )
            logger.info(
                f"Режим удаления: {'Попытка У ВСЕХ' if delete_for_everyone else 'ТОЛЬКО У СЕБЯ'}."
            )
            if delete_for_everyone:
                logger.warning(
                    "В режиме 'У ВСЕХ' будет предпринята попытка удалить сообщения ДРУГИХ пользователей в ЛИЧНЫХ чатах (обычно безуспешно)."
                )

            target_display_name = "ВСЕ доступные личные чаты и беседы"
            if target_chat_obj:
                target_display_name = f"КОНКРЕТНЫЙ ЧАТ ({target_chat_obj.title or target_chat_obj.username or target_chat_obj.id})"

            keywords_display = keywords_config[:10]
            keywords_display_str = ", ".join(keywords_display)
            if len(keywords_config) > 10:
                keywords_display_str += f", ... (всего {len(keywords_config)})"

            confirm_color = colorama.Fore.RED if _COLORAMA_AVAILABLE else ""
            reset_color = colorama.Style.RESET_ALL if _COLORAMA_AVAILABLE else ""
            confirm = input(
                f"\nВы собираетесь искать сообщения с ключевыми словами: {keywords_display_str}\n"
                f"Цель: {target_display_name}\n"
                f"Режим: {deletion_mode_choice}\n"
                f"{confirm_color}ЭТО ДЕЙСТВИЕ НЕОБРАТИМО!{reset_color}\n"
                f"Вы хотите продолжить? (да/нет): "
            ).lower()
            if confirm != "да":
                logger.info("Операция отменена пользователем.")
                await client.stop()
                return

        except (KeyboardInterrupt, EOFError):
            logger.warning("Настройка отменена пользователем.")
            await client.stop()
            return
        except Exception as e:
            logger.error(f"Произошла ошибка во время настройки: {e}")
            await client.stop()
            return

        dialogs_to_process: List[Tuple[Union[int, str], str, ChatType]] = []
        if target_chat_obj:
            if not target_chat_obj.type:
                logger.error(
                    f"Не удалось определить тип чата для {target_chat_obj.id}. Пропуск."
                )
            else:
                chat_id = target_chat_obj.id
                chat_name_log = (
                    target_chat_obj.title
                    or target_chat_obj.username
                    or f"ID: {chat_id}"
                )
                dialogs_to_process.append(
                    (chat_id, chat_name_log, target_chat_obj.type)
                )
                logger.info(
                    f"Цель установлена на конкретный чат: {chat_name_log} (Тип: {target_chat_obj.type.name})"
                )
        else:
            logger.info(
                "Получение списка всех диалогов (личные, группы, супергруппы)..."
            )
            try:
                dialog_count = 0
                async for dialog in client.get_dialogs():
                    stats["dialogs_found"] += 1
                    dialog_count += 1
                    chat = dialog.chat
                    dialog_name = chat.title or chat.username or f"Чат ID {chat.id}"

                    allowed_types = {
                        ChatType.PRIVATE,
                        ChatType.GROUP,
                        ChatType.SUPERGROUP,
                    }

                    if chat and chat.id and chat.type in allowed_types:
                        dialogs_to_process.append((chat.id, dialog_name, chat.type))
                    else:
                        stats["dialogs_skipped_type"] += 1
                        logger.debug(
                            f"Пропуск диалога '{dialog_name}' (ID: {chat.id}, Тип: {chat.type.name if chat.type else 'N/A'}) - неподдерживаемый тип."
                        )

                    if dialog_count % 100 == 0:
                        logger.debug(f"Обработано {dialog_count} диалогов...")
                        await asyncio.sleep(0.1)

                logger.info(
                    f"Найдено {len(dialogs_to_process)} подходящих чатов/бесед для сканирования (из {stats['dialogs_found']} всего диалогов, пропущено {stats['dialogs_skipped_type']} неподходящих типов)."
                )
            except errors.FloodWait as e:
                wait_time = e.value + 2
                logger.warning(
                    f"[FloodWait] Превышен лимит запросов при получении диалогов. Ожидание {wait_time} секунд."
                )
                await asyncio.sleep(wait_time)
                logger.warning("Список диалогов может быть неполным из-за FloodWait.")
            except Exception as e:
                logger.error(
                    f"Произошла ошибка при получении диалогов: {e}. Обработка может быть неполной.",
                    exc_info=True,
                )

        total_dialogs = len(dialogs_to_process)
        if total_dialogs == 0:
            logger.warning("Не найдено диалогов для обработки.")
            await client.stop()
            return

        logger.info(f"Начинается обработка {total_dialogs} диалогов...")
        batch_size_delete = 100
        api_search_delay = 0.5

        for i, (chat_id, chat_name, chat_type) in enumerate(dialogs_to_process, 1):
            stats["chats_processed"] += 1
            log_chat_ref = f"'{chat_name}' (ID: {chat_id}, Тип: {chat_type.name})"
            logger.info(f"\n--- Обработка чата {i}/{total_dialogs}: {log_chat_ref} ---")

            own_ids_to_delete: Set[int] = set()
            other_ids_to_delete: Set[Tuple[int, str]] = set()
            found_own_in_chat = 0
            found_other_in_chat = 0
            chat_processing_error = False

            try:
                for keyword_text in keywords_config:
                    if chat_processing_error:
                        break

                    logger.debug(
                        f"API поиск для '{keyword_text}' в чате {log_chat_ref}..."
                    )
                    messages_iterator = None

                    try:
                        messages_iterator = client.search_messages(
                            chat_id, query=keyword_text
                        )

                        messages_found_for_keyword_own = 0
                        messages_found_for_keyword_other = 0
                        processed_count_api = 0

                        async for message in messages_iterator:
                            if (
                                not message
                                or not hasattr(message, "id")
                                or not message.from_user
                            ):
                                logger.debug(
                                    f"Пропуск невалидного объекта сообщения в {log_chat_ref}"
                                )
                                continue

                            stats["total_checked_api"] += 1
                            processed_count_api += 1

                            if not isinstance(message.id, int) or message.id <= 0:
                                logger.warning(
                                    f"Найдено сообщение с невалидным ID ({message.id}) в {log_chat_ref}. Пропущено."
                                )
                                continue

                            if message.from_user.id == my_id:
                                if message.id not in own_ids_to_delete:
                                    stats["total_found_own"] += 1
                                    found_own_in_chat += 1
                                    messages_found_for_keyword_own += 1
                                    logger.debug(
                                        f"Найдено СВОЕ сообщение ID {message.id} ('{keyword_text}') в {log_chat_ref}. Добавлено к удалению."
                                    )
                                    own_ids_to_delete.add(message.id)

                            elif delete_for_everyone and chat_type == ChatType.PRIVATE:
                                if message.id not in {
                                    mid for mid, kw in other_ids_to_delete
                                }:
                                    stats["total_found_other"] += 1
                                    found_other_in_chat += 1
                                    messages_found_for_keyword_other += 1
                                    logger.debug(
                                        f"Найдено ЧУЖОЕ сообщение ID {message.id} ('{keyword_text}') в ЛИЧНОМ чате {log_chat_ref}. Добавлено к ПОПЫТКЕ удаления."
                                    )
                                    other_ids_to_delete.add((message.id, keyword_text))

                            if len(own_ids_to_delete) >= batch_size_delete:
                                batch_list = sorted(list(own_ids_to_delete))
                                logger.debug(
                                    f"Накоплен пакет из {len(batch_list)} ВАШИХ сообщений. Удаление в {log_chat_ref}..."
                                )
                                success = await delete_batch_own_messages(
                                    client,
                                    chat_id,
                                    chat_name,
                                    batch_list,
                                    delete_for_everyone,
                                )
                                if success:
                                    own_ids_to_delete.clear()
                                    logger.debug(
                                        f"Пакет ВАШИХ сообщений удален, очистка очереди для {log_chat_ref}."
                                    )
                                    await asyncio.sleep(1.5)
                                else:
                                    logger.error(
                                        f"Ошибка при удалении пакета ВАШИХ сообщений в {log_chat_ref}. Прерывание обработки этого чата."
                                    )
                                    chat_processing_error = True
                                    break

                            if processed_count_api % 500 == 0:
                                await asyncio.sleep(0.05)

                        if messages_found_for_keyword_own > 0:
                            logger.info(
                                f"Найдено {messages_found_for_keyword_own} СВОИХ сообщений ('{keyword_text}') в чате {log_chat_ref}."
                            )
                        if messages_found_for_keyword_other > 0:
                            logger.info(
                                f"Найдено {messages_found_for_keyword_other} ЧУЖИХ сообщений ('{keyword_text}') в ЛИЧНОМ чате {log_chat_ref} (будет попытка удаления)."
                            )

                        await asyncio.sleep(api_search_delay)

                    except errors.FloodWait as e:
                        wait_time = e.value + 5
                        logger.warning(
                            f"[FloodWait] Превышен лимит при поиске '{keyword_text}' в {log_chat_ref}. Ожидание {wait_time} секунд."
                        )
                        await asyncio.sleep(wait_time)
                        logger.warning(
                            f"Возобновление поиска в {log_chat_ref} после ожидания. Часть сообщений могла быть пропущена."
                        )
                        continue
                    except (
                        errors.ChannelPrivate,
                        errors.ChatForbidden,
                        errors.UserIsBlocked,
                        errors.ChatAdminRequired,
                        errors.PeerIdInvalid,
                    ) as e:
                        logger.warning(
                            f"ОШИБКА ДОСТУПА к {log_chat_ref}: {type(e).__name__}. Пропуск этого чата."
                        )
                        chat_processing_error = True
                        break
                    except errors.SearchQueryEmpty:
                        logger.warning(
                            f"Пустой или некорректный поисковый запрос для '{keyword_text}' в {log_chat_ref}. Пропуск слова."
                        )
                        continue
                    except Exception as iter_err:
                        logger.error(
                            f"Неожиданная ошибка при поиске/итерации '{keyword_text}' в {log_chat_ref}: {type(iter_err).__name__} - {iter_err}. Прерывание для чата."
                        )
                        chat_processing_error = True
                        break

                if other_ids_to_delete and not chat_processing_error:
                    logger.debug(
                        f"Начинаю ПОПЫТКУ удаления {len(other_ids_to_delete)} ЧУЖИХ сообщений в ЛИЧНОМ чате {log_chat_ref} (ожидаются ошибки)..."
                    )
                    sorted_other_ids = sorted(
                        list(other_ids_to_delete), key=lambda x: x[0]
                    )
                    for other_msg_id, keyword in sorted_other_ids:
                        await attempt_delete_other_message(
                            client, chat_id, chat_name, other_msg_id, keyword
                        )
                        await asyncio.sleep(0.5)
                    logger.debug(
                        f"Завершена ПОПЫТКА удаления {len(other_ids_to_delete)} ЧУЖИХ сообщений в {log_chat_ref}."
                    )

                if own_ids_to_delete and not chat_processing_error:
                    final_batch_list = sorted(list(own_ids_to_delete))
                    logger.debug(
                        f"Удаление последнего пакета из {len(final_batch_list)} ВАШИХ сообщений в {log_chat_ref}..."
                    )
                    success = await delete_batch_own_messages(
                        client,
                        chat_id,
                        chat_name,
                        final_batch_list,
                        delete_for_everyone,
                    )
                    if success:
                        own_ids_to_delete.clear()
                    else:
                        logger.error(
                            f"Ошибка при удалении финального пакета ВАШИХ сообщений в {log_chat_ref}."
                        )
                        chat_processing_error = True

            except Exception as chat_err:
                logger.error(
                    f"НЕОЖИДАННАЯ ОШИБКА при обработке чата {log_chat_ref} (вне цикла ключевых слов): {type(chat_err).__name__} - {chat_err}. Пропуск чата.",
                    exc_info=False,
                )
                chat_processing_error = True

            if chat_processing_error:
                stats["chats_failed"] += 1
                logger.error(
                    f"--- Завершена обработка чата {log_chat_ref} С ОШИБКАМИ. ---"
                )
            elif found_own_in_chat == 0 and found_other_in_chat == 0:
                logger.info(
                    f"--- Завершена обработка чата {log_chat_ref}. Подходящих сообщений (своих или чужих в ЛС) не найдено. ---"
                )
            else:
                log_parts = []
                if found_own_in_chat > 0:
                    log_parts.append(
                        f"Найдено и обработано {found_own_in_chat} СВОИХ сообщений"
                    )
                if found_other_in_chat > 0:
                    log_parts.append(
                        f"Найдено и предпринята попытка удаления {found_other_in_chat} ЧУЖИХ сообщений (в ЛС)"
                    )
                logger.info(
                    f"--- Успешно завершена обработка чата {log_chat_ref}. {'. '.join(log_parts)}. ---"
                )

            await asyncio.sleep(1)

        logger.info("\n" + "=" * 50)
        logger.info("         Процесс удаления ЗАВЕРШЕН")
        logger.info("=" * 50)
        logger.info(f"Обработано чатов/бесед: {stats['chats_processed']}")
        if stats["chats_failed"] > 0:
            logger.warning(f"Чатов завершено с ошибками: {stats['chats_failed']}")
        logger.info(f"Проверено сообщений (API поиск): {stats['total_checked_api']}")
        logger.info(
            f"Всего найдено СВОИХ сообщений по словам: {stats['total_found_own']}"
        )
        if stats["total_found_other"] > 0:
            logger.info(
                f"Всего найдено ЧУЖИХ сообщений (в ЛС) по словам: {stats['total_found_other']}"
            )

        cyan = colorama.Fore.CYAN if _COLORAMA_AVAILABLE else ""
        blue = colorama.Fore.BLUE if _COLORAMA_AVAILABLE else ""
        yellow = colorama.Fore.YELLOW if _COLORAMA_AVAILABLE else ""
        red = colorama.Fore.RED if _COLORAMA_AVAILABLE else ""
        magenta = colorama.Fore.MAGENTA if _COLORAMA_AVAILABLE else ""
        reset = colorama.Style.RESET_ALL if _COLORAMA_AVAILABLE else ""

        logger.info(
            f"{cyan}Успешно удалено СВОИХ У ВСЕХ: {stats['deleted_for_all']}{reset}"
        )
        logger.info(
            f"{blue}Успешно удалено СВОИХ ТОЛЬКО У СЕБЯ: {stats['deleted_for_me']}{reset}"
        )
        logger.info(
            f"{yellow}Не удалось у всех, но удалено СВОИХ У СЕБЯ: {stats['failed_revoke_but_deleted_for_me']}{reset}"
        )
        logger.error(
            f"{red}Не удалось удалить СВОИ полностью (ошибки/запреты): {stats['failed_to_delete_own']}{reset}"
        )
        if stats["attempted_delete_other"] > 0:
            logger.info(
                f"{magenta}Предпринято попыток удалить ЧУЖИХ сообщений (в ЛС): {stats['attempted_delete_other']}{reset}"
            )
            logger.warning(
                f"{red}Не удалось удалить ЧУЖИХ сообщений (ожидаемо): {stats['failed_to_delete_other']}{reset}"
            )
        logger.info("=" * 50)

        logger.info("Остановка клиента Pyrogram...")
        await client.stop()
        logger.info("Клиент Pyrogram остановлен.")

    except (
        errors.AuthKeyUnregistered,
        errors.AuthKeyInvalid,
        errors.UserDeactivated,
        errors.UserDeactivatedBan,
    ) as e:
        logger.error(
            f"Ошибка авторизации ({type(e).__name__}). Сессия повреждена или аккаунт неактивен."
        )
        logger.info(
            f"Попробуйте удалить файл сессии '{SESSION_NAME}.session' и запустить скрипт заново."
        )
    except errors.ApiIdInvalid:
        logger.error("Неверный API ID. Проверьте правильность ввода.")
    except errors.ApiIdPublishedFlood:
        logger.error(
            "Этот API ID использовался слишком часто недавно. Попробуйте другой или подождите."
        )
    except errors.PhoneNumberInvalid:
        logger.error("Указан неверный номер телефона при входе.")
    except errors.PhoneCodeInvalid:
        logger.error("Введен неверный код подтверждения Telegram.")
    except errors.PhoneCodeExpired:
        logger.error(
            "Срок действия кода подтверждения Telegram истек. Попробуйте войти снова."
        )
    except errors.PasswordHashInvalid:
        logger.error("Неверный пароль двухфакторной аутентификации (облачный пароль).")
    except errors.FloodWait as e:
        wait_time = e.value + 2
        logger.error(
            f"[FloodWait] во время запуска или первоначальной настройки: ожидание {wait_time} секунд. Прерывание."
        )
    except ConnectionError:
        logger.error("Ошибка соединения с Telegram. Проверьте подключение к интернету.")
    except (KeyboardInterrupt, EOFError):
        logger.info("\nСкрипт прерван пользователем во время работы.")
    except Exception as main_err:
        logger.critical(
            f"Произошла КРИТИЧЕСКАЯ ошибка во время выполнения: {type(main_err).__name__} - {main_err}",
            exc_info=True,
        )
    finally:
        if (
            "client" in locals()
            and isinstance(client, Client)
            and client.is_initialized
            and client.is_connected
        ):
            logger.info("Попытка остановить клиент в блоке finally...")
            try:
                await client.stop()
                logger.info("Клиент остановлен.")
            except Exception as stop_err:
                logger.warning(
                    f"Ошибка при остановке клиента после завершения: {stop_err}"
                )
        if _COLORAMA_AVAILABLE:
            print(colorama.Style.RESET_ALL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано пользователем перед запуском async.")
    finally:
        if _COLORAMA_AVAILABLE:
            print(colorama.Style.RESET_ALL)
