#!/usr/bin/env python3
"""
Generate a Fernet encryption key for OAuth token encryption.
Run this script to generate a key, then add it to your .env file as ENCRYPTION_KEY.
"""
from cryptography.fernet import Fernet

if __name__ == "__main__":
    key = Fernet.generate_key()
    print("=" * 60)
    print("Generated ENCRYPTION_KEY:")
    print("=" * 60)
    print(key.decode())
    print("=" * 60)
    print("\nAdd this to your .env file:")
    print(f"ENCRYPTION_KEY={key.decode()}")
    print("\n⚠️  IMPORTANT:")
    print("   - Keep this key secret and never commit it to version control")
    print("   - If you change this key, all existing OAuth tokens will need to be reconnected")
    print("   - Make sure to set this BEFORE connecting any OAuth accounts")
    print("=" * 60)
