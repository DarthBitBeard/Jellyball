"""Chunk E1 (2.2.0): secrets encrypted at rest, built-in TLS, credential rotation.

Covers db.SECRET_SETTING_KEYS encryption (roundtrip, no plaintext in the DB
file, legacy plaintext migration, corrupt ciphertext), config TLS cert
generation, and the security.py rotation helpers.
"""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import config
import db
import security


def _freshen_db_modules(tmpdir):
    """Point db at a temp database + temp encryption key file."""
    db_path = str(Path(tmpdir) / "test.db")
    key_path = Path(tmpdir) / "settings-encryption.key"
    db.close_all_db_connections()
    db._FERNET_BY_KEYFILE.clear()
    db.DB_FILE = db_path
    db._SETTINGS_ENCRYPTION_KEY_FILE = key_path
    db.init_db()
    return db_path, key_path


class SecretEncryptionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path, self.key_path = _freshen_db_modules(self.tmp.name)

    def tearDown(self):
        db.close_all_db_connections()
        db._FERNET_BY_KEYFILE.clear()
        self.tmp.cleanup()

    def _raw_value(self, key):
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def test_secret_roundtrip(self):
        db.set_setting("jellyfin_api_key", "super-secret-key")
        self.assertEqual(db.get_setting("jellyfin_api_key"), "super-secret-key")
        db.set_setting("telegram_bot_token", "123:ABC")
        self.assertEqual(db.get_setting("telegram_bot_token"), "123:ABC")
        db.set_setting("discord_webhook_url", "https://discord.example/hook")
        self.assertEqual(db.get_setting("discord_webhook_url"), "https://discord.example/hook")

    def test_plaintext_never_hits_db_file(self):
        secrets = {
            "jellyfin_api_key": "jf-api-key-xyz",
            "telegram_bot_token": "tg-token-xyz",
            "discord_webhook_url": "https://discord.example/xyz",
        }
        for key, value in secrets.items():
            db.set_setting(key, value)
        raw = Path(self.db_path).read_bytes()
        for value in secrets.values():
            self.assertNotIn(value.encode("utf-8"), raw,
                             "plaintext secret found in the database file")
        for key in secrets:
            stored = self._raw_value(key)
            self.assertTrue(stored.startswith(db._SETTINGS_ENCRYPTED_MARKER),
                            f"{key} is not stored encrypted")
            self.assertEqual(db.get_setting(key), secrets[key])

    def test_non_secret_settings_stay_plaintext(self):
        db.set_setting("jellyfin_url", "http://localhost:8096")
        self.assertEqual(self._raw_value("jellyfin_url"), "http://localhost:8096")
        self.assertEqual(db.get_setting("jellyfin_url"), "http://localhost:8096")

    def test_empty_secret_stored_as_empty(self):
        db.set_setting("jellyfin_api_key", "")
        self.assertEqual(self._raw_value("jellyfin_api_key"), "")
        self.assertEqual(db.get_setting("jellyfin_api_key"), "")

    def test_legacy_plaintext_migrates_on_read(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("INSERT INTO app_settings (key, value) VALUES (?, ?)",
                         ("jellyfin_api_key", "legacy-plaintext-key"))
            conn.commit()
        finally:
            conn.close()
        # Read returns the plaintext and transparently rewrites it encrypted.
        self.assertEqual(db.get_setting("jellyfin_api_key"), "legacy-plaintext-key")
        stored = self._raw_value("jellyfin_api_key")
        self.assertTrue(stored.startswith(db._SETTINGS_ENCRYPTED_MARKER))
        self.assertNotIn(b"legacy-plaintext-key", Path(self.db_path).read_bytes())

    def test_corrupt_ciphertext_returns_default(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("INSERT INTO app_settings (key, value) VALUES (?, ?)",
                         ("telegram_bot_token", db._SETTINGS_ENCRYPTED_MARKER + "garbage"))
            conn.commit()
        finally:
            conn.close()
        self.assertEqual(db.get_setting("telegram_bot_token", "fallback"), "fallback")

    def test_missing_key_returns_default(self):
        self.assertEqual(db.get_setting("jellyfin_api_key", "dflt"), "dflt")

    def test_key_file_created_with_restricted_permissions(self):
        db.set_setting("jellyfin_api_key", "x")
        self.assertTrue(self.key_path.exists())
        self.assertEqual(len(self.key_path.read_bytes()), 32)
        if os.name != "nt":
            self.assertEqual(oct(self.key_path.stat().st_mode & 0o777), "0o600")

    def test_no_double_encryption(self):
        db.set_setting("jellyfin_api_key", "abc")
        stored_once = self._raw_value("jellyfin_api_key")
        # Passing an already-encrypted value stores it verbatim, not wrapped twice.
        db.set_setting("jellyfin_api_key", stored_once)
        self.assertEqual(self._raw_value("jellyfin_api_key"), stored_once)
        self.assertEqual(db.get_setting("jellyfin_api_key"), "abc")


class TlsCertTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def test_cert_generation_and_reuse(self):
        cert_file = Path(self.tmp.name) / "tls-cert.pem"
        key_file = Path(self.tmp.name) / "tls-key.pem"
        with mock.patch.object(config, "TLS_ENABLED", True), \
             mock.patch.object(config, "TLS_CERT_FILE", cert_file), \
             mock.patch.object(config, "TLS_KEY_FILE", key_file):
            first = config._ensure_tls_cert()
            self.assertIsNotNone(first)
            self.assertTrue(cert_file.exists())
            self.assertTrue(key_file.exists())
            if os.name != "nt":
                self.assertEqual(oct(key_file.stat().st_mode & 0o777), "0o600")
            cert_text = cert_file.read_text()
            self.assertIn("BEGIN CERTIFICATE", cert_text)
            self.assertIn("BEGIN RSA PRIVATE KEY", key_file.read_text())
            mtime = cert_file.stat().st_mtime
            second = config._ensure_tls_cert()
            self.assertEqual(first, second)
            self.assertEqual(cert_file.stat().st_mtime, mtime,
                             "existing cert pair must be reused, not regenerated")

    def test_cert_subject_and_san(self):
        from cryptography import x509
        cert_file = Path(self.tmp.name) / "tls-cert.pem"
        key_file = Path(self.tmp.name) / "tls-key.pem"
        with mock.patch.object(config, "TLS_ENABLED", True), \
             mock.patch.object(config, "TLS_CERT_FILE", cert_file), \
             mock.patch.object(config, "TLS_KEY_FILE", key_file):
            config._ensure_tls_cert()
        cert = x509.load_pem_x509_certificate(cert_file.read_bytes())
        self.assertEqual(cert.subject.rfc4514_string(), "CN=Jellyball")
        sans = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        dns_names = sans.get_values_for_type(x509.DNSName)
        self.assertIn("localhost", dns_names)

    def test_tls_disabled_returns_none(self):
        with mock.patch.object(config, "TLS_ENABLED", False):
            self.assertIsNone(config._ensure_tls_cert())


class RotationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def test_regenerate_dashboard_password(self):
        pw_file = Path(self.tmp.name) / "dashboard-password.txt"
        old_password = security.DASHBOARD_PASSWORD
        old_mode = security.DASHBOARD_AUTH_MODE
        try:
            with mock.patch.object(security, "DASHBOARD_PASSWORD_FILE", pw_file), \
                 mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("DASHBOARD_PASSWORD", None)
                new_password = security.regenerate_dashboard_password()
            self.assertEqual(len(new_password), 32,
                             "token_urlsafe(24) must produce a 32-char password")
            self.assertEqual(pw_file.read_text(encoding="utf-8").strip(), new_password)
            self.assertEqual(security.DASHBOARD_PASSWORD, new_password)
            if os.name != "nt":
                self.assertEqual(oct(pw_file.stat().st_mode & 0o777), "0o600")
        finally:
            security.DASHBOARD_PASSWORD = old_password
            security.DASHBOARD_AUTH_MODE = old_mode

    def test_rotate_relay_signing_key(self):
        key_file = Path(self.tmp.name) / "relay-signing.key"
        old_key = security._RELAY_SIGNING_KEY
        try:
            with mock.patch.object(security, "_relay_signing_key_file", lambda: key_file):
                old_sig = security._relay_signature("http://example/x", "r", "o")
                security.rotate_relay_signing_key()
                self.assertNotEqual(security._RELAY_SIGNING_KEY, old_key)
                self.assertTrue(key_file.exists())
                # Old signatures no longer validate; new ones do.
                self.assertFalse(security._relay_signature_ok("http://example/x", "r", "o", old_sig))
                new_sig = security._relay_signature("http://example/x", "r", "o")
                self.assertTrue(security._relay_signature_ok("http://example/x", "r", "o", new_sig))
        finally:
            security._RELAY_SIGNING_KEY = old_key


if __name__ == "__main__":
    unittest.main()
