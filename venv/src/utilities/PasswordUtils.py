import base64


class PasswordUtils:

    @staticmethod
    def encode_password(password):
        encoded = base64.b64encode(password.encode())
        return encoded.decode()

    @staticmethod
    def decode_password(encoded_password):
        return base64.b64decode(encoded_password).decode()
