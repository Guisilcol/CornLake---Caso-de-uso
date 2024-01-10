from random import choice, choices
from string import ascii_lowercase as ASCII_LOWERCASE, ascii_uppercase as ASCII_UPPERCASE
from datetime import datetime as Datetime
from uuid import uuid4 
from faker import Faker
from faker_food import FoodProvider
from pandas import Series, DataFrame, concat
from typing import TypedDict

_GENERATOR = Faker('pt-BR')
_GENERATOR.add_provider(FoodProvider)
"""A generator object to generate random data"""

class FieldFunctionKwargs(TypedDict):
    """A type hint for the aditional params of the field function. Is variable depending on the type of the field. 
    
    Attributes:
        length (int): The length of the string to be generated. Ex: 100
        min_value (int): The minimum value of the integer to be generated. Ex: 0
        max_value (int): The maximum value of the integer to be generated. Ex: 100
        min_date (str): The minimum date of the datetime to be generated. Ex: '2000-01-01 00:00:00'
        max_date (str): The maximum date of the datetime to be generated. Ex: '2021-01-01 00:00:00'
    """
    length: int 
    min_value: int
    max_value: int
    min_date: str
    max_date: str
    now_date: bool
    
class DataGenerator: 
    
    GENERATOR: Faker = _GENERATOR
    
    @classmethod
    def generate_string(cls, params: FieldFunctionKwargs) -> str:
        length = params.get('length', 10)
        return ''.join(choices(ASCII_LOWERCASE + ASCII_UPPERCASE, k=length))
    
    @classmethod
    def generate_uuid(cls) -> str:
        return str(uuid4())

    @classmethod
    def generate_name(cls) -> str: 
        return cls.GENERATOR.name()

    @classmethod
    def generate_product_name(cls) -> str: 
        return choice([
            cls.GENERATOR.dish()
            ,cls.GENERATOR.fruit()
            ,cls.GENERATOR.spice()
            ,cls.GENERATOR.sushi()
            ,cls.GENERATOR.vegetable()
        ])
        
    @classmethod
    def generate_address(cls) -> str: 
        return cls.GENERATOR.address()

    @classmethod
    def generate_datetime(cls, params: FieldFunctionKwargs) -> str:
        
        now = params.get('now_date', False)
        
        if now:
            return Datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        min_date = params.get('min_date', '2000-01-01 00:00:00')
        max_date = params.get('max_date', Datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        return cls.GENERATOR.date_time_between_dates(
            Datetime.strptime(min_date, "%Y-%m-%d %H:%M:%S"), 
            Datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S")
        ).strftime('%Y-%m-%d %H:%M:%S')
    
    @classmethod
    def generate_integer(cls, params: FieldFunctionKwargs) -> int:
        min_value = params.get('min_value', 0)
        max_value = params.get('max_value', 100)
        return choice(range(min_value, max_value))

    @classmethod
    def generate_float(cls, params: FieldFunctionKwargs) -> float:
        min_value = params.get('min_value', 0)
        max_value = params.get('max_value', 100) 
        return choice(range(min_value, max_value)) + choice([0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])

    @classmethod
    def generate_boolean(cls) -> bool:
        return choice([True, False])

DATA_TYPE_MAPPING = {
    'string':                   ('object',          lambda params: DataGenerator.generate_string(params)),
    'name':                     ('object',          lambda params: DataGenerator.generate_name()),
    'product':                  ('object',          lambda params: DataGenerator.generate_product_name()),
    'address':                  ('object',          lambda params: DataGenerator.generate_address()),
    'integer':                  ('int64',           lambda params: DataGenerator.generate_integer(params)),
    'float':                    ('float64',         lambda params: DataGenerator.generate_float(params)),
    'boolean':                  ('bool',            lambda params: DataGenerator.generate_boolean(params)),
    'datetime':                 ('datetime64[ns]',  lambda params: DataGenerator.generate_datetime(params)),
    'uuid':                     ('object',          lambda params: DataGenerator.generate_uuid())
}
    
def field(
        name: str, 
        type: str, 
        quantity: int, 
        can_generate_null: bool = False, 
        possible_random_values: list | None = None,
        kwargs: FieldFunctionKwargs = {}) -> Series:
    
    mapping = DATA_TYPE_MAPPING.get(type)
    
    if mapping == None:
        raise Exception(f'The type {type} is not supported')
    
    dtype = mapping[0]
    generator = mapping[1]
    
    if possible_random_values != None and len(possible_random_values) > 0:
        return Series(
            data=[choice(possible_random_values) for _ in range(quantity)], 
            dtype=dtype, 
            name=name
        )    
    if can_generate_null: 
        return Series(
            data=[generator(kwargs) if choice([True, False]) else None for _ in range(quantity)], 
            dtype=dtype, 
            name=name
        )
    
    return Series(
        data=[generator(kwargs) for _ in range(quantity)], 
        dtype=dtype, 
        name=name
    )
        
def generate_dataframe(fields: list[Series]) -> DataFrame:
    return concat(fields, axis=1)
        
    
    
