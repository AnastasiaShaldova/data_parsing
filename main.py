import json
from concurrent.futures import ThreadPoolExecutor
from zipfile import ZipFile


class CompanyDataParser:
    def __init__(self, zip_file):
        self.zip_file_path = zip_file  # Инициализация пути к ZIP-файлу

    def parse_companies(self):
        with ThreadPoolExecutor() as executor:  # Создание пула потоков для выполнения задач
            with ZipFile(self.zip_file_path, 'r') as zip_file:  # Открытие ZIP-файла в режиме чтения
                file_names = zip_file.namelist()  # Получение списка имен файлов внутри ZIP-архива
                chunks = [file_names[i:i + 1000] for i in
                          range(0, len(file_names), 1000)]  # Разбиение списка имен файлов на порции по 1000 штук
                for chunk in chunks:  # Проход по порциям имен файлов
                    for file_name in chunk:  # Проход по именам файлов в порции
                        with zip_file.open(file_name) as json_file:  # Открытие файла внутри ZIP-архива в режиме чтения
                            json_data = json.load(json_file)  # Загрузка JSON-данных из файла
                            for company_json in json_data:  # Проход по объектам компаний в JSON-данных
                                task = executor.submit(self._extract_company_data,
                                                       company_json)  # Подача задачи на извлечение данных о компании в пул потоков
                                self._print_company(task.result())  # Печать результата выполнения задачи

    def _extract_company_data(self, company_json):
        city_name = 'ХАБАРОВСК'
        address = self._get_nested_value(company_json,
                                         'НаимГород')  # Извлечение значения поля 'НаимГород' из JSON-объекта компании
        okved = self._get_nested_value(company_json,
                                       'КодОКВЭД')  # Извлечение значения поля 'КодОКВЭД' из JSON-объекта компании

        if address is not None and city_name in address and okved is not None and okved.startswith('62'):
            company = {
                'inn': company_json['inn'],
                'kpp': company_json['kpp'],
                'name': company_json['name'],
                'full_name': company_json['full_name'],
                'okved': okved,
                'address': company_json['data']['СвРегОрг']['АдрРО']
            }
            return company  # Возврат словаря с данными о компании

    def _get_nested_value(self, json_data, key):
        if isinstance(json_data, dict):  # Проверка, является ли объект словарем
            for k, v in json_data.items():  # Проход по парам ключ-значение словаря
                if k == key:  # Проверка, соответствует ли ключ заданному ключу
                    return v
                elif isinstance(v, dict) or isinstance(v, list):  # Проверка, является ли значение словарем или списком
                    result = self._get_nested_value(v, key)  # Рекурсивный вызов для поиска во вложенных объектах
                    if result is not None:  # Если найдено значение, вернуть его
                        return result
        elif isinstance(json_data, list):  # Проверка, является ли объект списком
            for item in json_data:  # Проход по элементам списка
                result = self._get_nested_value(item, key)  # Рекурсивный вызов для поиска во вложенных объектах
                if result is not None:  # Если найдено значение, вернуть его
                    return result
        return None

    def _print_company(self, company):
        if company is not None:
            print(company)


def main():
    zip_file_path = 'egrul.json.zip'  # Путь к ZIP-файлу
    parser = CompanyDataParser(zip_file_path)  # Создание экземпляра парсера с указанным путем к ZIP-файлу
    parser.parse_companies()  # Запуск процесса парсинга компаний


if __name__ == '__main__':
    main()
