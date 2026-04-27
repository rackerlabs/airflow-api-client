"""Tests for profile command"""

import configparser
from unittest.mock import MagicMock, patch

import pytest

from airflow_api_client.commands.profile import ProfileCommand

# pylint: disable=protected-access


def _make_app(**kwargs):
    """Create a mock App with profile args"""
    defaults = {
        "profile_action": "create",
        "profile_name": "test_profile",
        "endpoint": "https://airflow.test.example.com",
        "username": "testuser",
        "password": None,
        "no_keyring_password": False,
        "config_file": None,
    }
    defaults.update(kwargs)
    app = MagicMock()
    args = MagicMock()
    for key, value in defaults.items():
        setattr(args, key, value)
    app.args = args
    return app


class TestProfileCreate:
    """Tests for profile create subcommand"""

    def test_create_profile_without_keyring(self, tmp_path):
        """Test creating a profile stores password in config when keyring not available"""
        app = _make_app(password="secret123")

        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.keyring", None),
        ):
            cmd._create()

        config = configparser.RawConfigParser()
        config.read(tmp_path / "airflow-api-client" / "config.ini")

        assert config.get("test_profile", "endpoint") == "https://airflow.test.example.com"
        assert config.get("test_profile", "username") == "testuser"
        assert config.get("test_profile", "password") == "secret123"

    def test_create_profile_overwrites_existing(self, tmp_path):
        """Test that creating an existing profile overwrites it"""
        config_dir = tmp_path / "airflow-api-client"
        config_dir.mkdir(parents=True)
        config_path = config_dir / "config.ini"

        config = configparser.RawConfigParser()
        config.add_section("test_profile")
        config.set("test_profile", "endpoint", "https://old.example.com")
        config.set("test_profile", "username", "olduser")
        with config_path.open("w") as f:
            config.write(f)

        app = _make_app(password="newpass")

        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.keyring", None),
        ):
            cmd._create()

        config = configparser.RawConfigParser()
        config.read(config_path)
        assert config.get("test_profile", "endpoint") == "https://airflow.test.example.com"
        assert config.get("test_profile", "username") == "testuser"

    def test_create_profile_with_keyring(self, tmp_path):
        """Test creating a profile with keyring stores in keyring, not config"""
        app = _make_app(password="secret123")

        mock_keyring = MagicMock()
        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.keyring", mock_keyring),
        ):
            cmd._create()

        mock_keyring.set_password.assert_called_once_with(
            "airflow-api-client/https://airflow.test.example.com", "testuser", "secret123"
        )

        # Password should NOT be in config file
        config = configparser.RawConfigParser()
        config.read(tmp_path / "airflow-api-client" / "config.ini")
        assert not config.has_option("test_profile", "password")

    def test_create_profile_prompts_for_missing_fields(self, tmp_path):
        """Test that missing endpoint/username are prompted for"""
        app = _make_app(endpoint=None, username=None, password="secret")

        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.keyring", None),
            patch("airflow_api_client.commands.profile.sys.stdin.isatty", return_value=True),
            patch("builtins.input", side_effect=["https://prompted.example.com", "prompted_user"]),
        ):
            cmd._create()

        config = configparser.RawConfigParser()
        config.read(tmp_path / "airflow-api-client" / "config.ini")
        assert config.get("test_profile", "endpoint") == "https://prompted.example.com"
        assert config.get("test_profile", "username") == "prompted_user"

    def test_create_profile_no_keyring_flag(self, tmp_path):
        """Test --no-keyring-password forces plaintext even when keyring available"""
        app = _make_app(password="secret123", no_keyring_password=True)

        mock_keyring = MagicMock()
        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.keyring", mock_keyring),
        ):
            cmd._create()

        mock_keyring.set_password.assert_not_called()

        config = configparser.RawConfigParser()
        config.read(tmp_path / "airflow-api-client" / "config.ini")
        assert config.get("test_profile", "password") == "secret123"

    def test_create_profile_custom_config_file(self, tmp_path):
        """Test --config-file writes to custom location"""
        config_path = tmp_path / "custom.ini"
        app = _make_app(password="secret123", config_file=config_path)

        cmd = ProfileCommand()
        cmd.app = app
        with patch("airflow_api_client.commands.profile.keyring", None):
            cmd._create()

        config = configparser.RawConfigParser()
        config.read(config_path)
        assert config.get("test_profile", "endpoint") == "https://airflow.test.example.com"

    def test_create_profile_non_interactive_missing_fields(self, tmp_path):
        """Test error when endpoint/username missing in non-interactive mode"""
        app = _make_app(endpoint=None, username=None, password="secret")

        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.sys") as mock_sys,
            pytest.raises(RuntimeError, match="non-interactive"),
        ):
            mock_sys.stdin.isatty.return_value = False
            cmd._create()


class TestProfileDelete:
    """Tests for profile delete subcommand"""

    def _write_profile(self, config_path, profile="test_profile"):
        """Helper to write a test profile"""
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config = configparser.RawConfigParser()
        config.add_section(profile)
        config.set(profile, "endpoint", "https://airflow.test.example.com")
        config.set(profile, "username", "testuser")
        with config_path.open("w") as f:
            config.write(f)

    def test_delete_profile(self, tmp_path):
        """Test deleting a profile removes it from config"""
        config_path = tmp_path / "airflow-api-client" / "config.ini"
        self._write_profile(config_path)

        app = _make_app(profile_action="delete", keep_keyring_password=False)

        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.keyring", None),
        ):
            cmd._delete()

        config = configparser.RawConfigParser()
        config.read(config_path)
        assert not config.has_section("test_profile")

    def test_delete_profile_with_keyring(self, tmp_path):
        """Test deleting a profile also removes keyring entry"""
        config_path = tmp_path / "airflow-api-client" / "config.ini"
        self._write_profile(config_path)

        app = _make_app(profile_action="delete", keep_keyring_password=False)

        mock_keyring = MagicMock()
        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.keyring", mock_keyring),
        ):
            cmd._delete()

        mock_keyring.delete_password.assert_called_once_with(
            "airflow-api-client/https://airflow.test.example.com", "testuser"
        )

    def test_delete_profile_keep_keyring(self, tmp_path):
        """Test --keep-keyring-password preserves keyring entry"""
        config_path = tmp_path / "airflow-api-client" / "config.ini"
        self._write_profile(config_path)

        app = _make_app(profile_action="delete", keep_keyring_password=True)

        mock_keyring = MagicMock()
        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.keyring", mock_keyring),
        ):
            cmd._delete()

        mock_keyring.delete_password.assert_not_called()

    def test_delete_nonexistent_profile(self, tmp_path):
        """Test deleting a profile that doesn't exist raises error"""
        config_path = tmp_path / "airflow-api-client" / "config.ini"
        self._write_profile(config_path, profile="other_profile")

        app = _make_app(profile_action="delete", keep_keyring_password=False)

        cmd = ProfileCommand()
        cmd.app = app
        with (
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(tmp_path)}),
            patch("airflow_api_client.commands.profile.keyring", None),
            pytest.raises(RuntimeError, match="not found"),
        ):
            cmd._delete()

    # Tests for password retrieval logic

    def test_password_from_args(self):
        """Test password from --password flag"""
        app = _make_app(password="fromargs")
        result = ProfileCommand().get_password(app.args, "https://example.com", "user")
        assert result == "fromargs"

    def test_password_from_stdin_pipe(self):
        """Test password from piped stdin"""
        app = _make_app()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            mock_stdin.readline.return_value = "piped_password\n"
            result = ProfileCommand().get_password(app.args, "https://example.com", "user")
        assert result == "piped_password"

    def test_password_from_stdin_interactive(self):
        """Test password from interactive getpass prompt"""
        app = _make_app()
        with (
            patch("sys.stdin") as mock_stdin,
            patch("airflow_api_client.commands.profile.getpass", return_value="typed_password"),
        ):
            mock_stdin.isatty.return_value = True
            result = ProfileCommand().get_password(app.args, "https://example.com", "user")
        assert result == "typed_password"

    def test_password_from_env(self):
        """Test password from environment variable"""
        app = _make_app()
        with patch.dict("os.environ", {"AIRFLOW_API_CLIENT_PASSWORD": "envpass"}):
            result = ProfileCommand().get_password(app.args, "https://example.com", "user")
        assert result == "envpass"
