from urllib.parse import urlencode
import pandas as pd
import numpy as np

import requests


BASE_URL = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'


def download_data(public_key: str, local_path: str = None, separ: str = ';') -> pd.DataFrame:
    """Функция позволяет загружать данные из указанного URL-адреса (с Яндекс Диска) или из локального файла,
    в зависимости от наличия успешного запроса.

    Args:
        public_key (str): публичный ключ, который используется для создания URL-адреса, с которого нужно загрузить данные.
        local_path (str, optional): (необязательный аргумент): путь к локальному файлу, который будет загружен, если запрос к URL-адресу неуспешен.
        separ (str, optional): разделитель, который будет использоваться для чтения загруженных данных. По умолчанию установлено значение ';'.

    Returns:
        pd.DataFrame: возвращает df из исходной таблицы csv
    """
    # получаем url
    final_url = BASE_URL + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    if response.ok:
        download_url_orders = response.json()['href']
        return pd.read_csv(download_url_orders, delimiter=separ)

    return pd.read_csv(local_path, delimiter=separ)


def collect_summary_df(
    groups_df: pd.DataFrame, groups_add_df: pd.DataFrame, active_studs_df: pd.DataFrame, checks_df: pd.DataFrame
) -> pd.DataFrame:
    """Функция позволяет автоматически подгружать информацию из дополнительного файла groups_add.csv (у которого могут отличаться заголовки)
    и формировать сводный датафрейм со всеми необходимыми данными для проыедения A/B теста.

    Args:
        groups_df (pd.DataFrame): датафрейм с информацией о принадлежности пользователя к контрольной или экспериментальной группе (А – контроль, B – целевая группа),
        groups_add_df (pd.DataFrame): датафрейм с пользователями, который прислали спустя несколько дней после передачи данных,
        active_studs_df (pd.DataFrame): датафрейм с информацией о пользователях, которые зашли на платформу в дни проведения эксперимента.
        checks_df (pd.DataFrame): датафрейм  с информацией об оплатах пользователей в дни проведения эксперимента.

    Returns:
        pd.DataFrame: возвращает общий df из всех df, необходимых для анализа
    """

    groups_full_df = pd.DataFrame(np.concatenate((groups_df.values, groups_add_df.values), axis=0))
    groups_full_df.columns = ['id', 'grp']
    groups_full_df.drop_duplicates()

    # Получаем таблицу с юзерами, которые были на платформе в день эксперимента
    experiment_users = pd.merge(groups_full_df, active_studs_df, how='right', left_on='id', right_on='student_id').drop(
        'student_id', axis=1
    )
    # Соединяем клиентов из эксперимента с df чеками
    summary_df = pd.merge(checks_df, experiment_users, how='right', left_on='student_id', right_on='id').drop(
        'student_id', axis=1
    )
    summary_df['rev'] = summary_df['rev'].fillna(0)
    # Создаем столбец лейб - конверсии
    summary_df['converted'] = np.where(summary_df.rev > 0, 1, 0)

    return summary_df


def consolidation_df(experiment_df) -> pd.DataFrame:
    """Функция возвращает df с расчитанными метриками эффетивности механики оплаты по экспериментальным группам

    Args:
        experiment_df (_type_): общий df со всеми данными для анализа

    Returns:
        pd.DataFrame: возвращает df с расчитанными метриками эффективности по экспериментальынм группам
    """
    # Считаем конвесии на каждого пользователя (на случай если есть повторные покупки)
    experiment_df_cr = (
        experiment_df.groupby(['grp', 'id'])
        .agg({'converted': 'sum'})
        .reset_index()
        .sort_values('converted', ascending=False)
    )
    # Считаем отдельно конверсии = 0 и 1
    cr_df = experiment_df_cr.pivot_table(index='grp', columns='converted', aggfunc='size', fill_value=0)
    cr_df.columns = ['converted_0', 'converted_1']
    cr_df = cr_df.reset_index()
    cr_df['total_conversions'] = cr_df['converted_0'] + cr_df['converted_1']
    # Получаем CR
    cr_df['conversion_rate'] = cr_df['converted_1'] / cr_df['total_conversions']
    # считаем доход по группам
    rev_4_grp_df = experiment_df.groupby('grp').agg(rev=('rev', 'sum'), users=('id', 'nunique')).reset_index()
    # считаем количество платящих пользователей
    paying_users_df = (
        experiment_df_cr.query('converted==1')
        .reset_index()
        .groupby('grp')
        .agg(paying_users=('id', 'nunique'))
        .reset_index()
    )
    # Добавляем к df столбцы с доходо и количеством пользователей
    consolidated_df = (cr_df.merge(rev_4_grp_df, on='grp')).merge(paying_users_df, on='grp')
    # # Считаем ARPAU
    consolidated_df['arpau'] = consolidated_df['rev'] / consolidated_df['users']
    # Считаем ARPPU
    consolidated_df['arppu'] = consolidated_df['rev'] / consolidated_df['paying_users']

    return consolidated_df
