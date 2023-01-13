from abc import ABC, abstractclassmethod

class idatabase(ABC):

    @abstractclassmethod
    def set_connection_string():   
        raise Exception("Should implement method: set_connection_string")
    
    @abstractclassmethod
    def set_engine():   
        raise Exception("Should implement method: set_engine")

    @abstractclassmethod
    def create_session():   
        raise Exception("Should implement method: create_session")