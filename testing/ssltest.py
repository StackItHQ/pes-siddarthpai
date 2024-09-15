import sys
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")

try:
    import ssl
    print("SSL module imported successfully")
    print(f"SSL version: {ssl.OPENSSL_VERSION}")
    print(f"SSL module location: {ssl.__file__}")
    print(f"SSL module dir: {dir(ssl)}")
except Exception as e:
    print(f"Error importing ssl: {e}")

try:
    from ssl import SSLError
    print("SSLError imported successfully")
except Exception as e:
    print(f"Error importing SSLError: {e}")