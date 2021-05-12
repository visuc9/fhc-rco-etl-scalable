# The purpose of this file is to manage password encryption and decryptions.

from cryptography.fernet import Fernet
import getpass
import os


# Encryption Class
class Encrypt:
    # def __init__(self):
    #     print('Encrypt Class Initialized')

    # Creates an encryption key, which can then be stored as an Environmental Variable on a Given Machine.
    def create_encryption_key(self):
        key = Fernet.generate_key().decode()
        print('\nNew Key = ' + str(key))
        print('\n')
        return key

    # Encrypt a password.  NOTE: Will be prompted in termal to enter the password, as we try to avoid any passwords
    # being hard coded.
    def encrypt_password(self, key):
        pw = getpass.getpass(prompt='\nPlease Enter Your Password: ')
        print('Your Password Was:', pw)
        epw = pw.encode()
        f = Fernet(key.encode())
        epw = f.encrypt(epw)
        epw = epw.decode("utf-8")
        print('Your Encrypted Password Is:', epw)
        print('\n')
        return epw

    # Decrypts password.
    def decrypt_password(self, epw, key):
        s = epw
        s = s.encode()
        f = Fernet(key)
        s = f.decrypt(s)
        s = s.decode()
        return s

# Instantiates an Encrypt Class.
# E = Encrypt()
# E.encrypt_password(os.environ.get('PythonEncryptionKey'))
# # Runs the method to generate an Encryption Key.
# E.create_encryption_key()

# # Encrypts a given password, which is prompted in terminal, based on the key stored as an environmental variable.
# key = os.environ.get('PythonEncryptionKey')
# E.encrypt_password(key)
def get_secret(secret, key):
    e = Encrypt()
    secret = e.decrypt_password(secret, key)
    return secret