"""Tests for backend/x_avatars.py: reading a picture URL out of an
api.fxtwitter.com response."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'backend'))

from x_avatars import avatar_from_fx, is_x_avatar  # noqa: E402


def test_normal_size_becomes_400():
    payload = {'code': 200, 'user': {'avatar_url': 'https://pbs.twimg.com/profile_images/1/abc_normal.jpg'}}
    assert avatar_from_fx(payload) == 'https://pbs.twimg.com/profile_images/1/abc_400x400.jpg'


def test_other_extensions_and_sizes():
    payload = {'user': {'avatar_url': 'https://pbs.twimg.com/profile_images/1/abc_bigger.png'}}
    assert avatar_from_fx(payload) == 'https://pbs.twimg.com/profile_images/1/abc_400x400.png'


def test_default_egg_and_missing_user():
    egg = {'user': {'avatar_url': 'https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png'}}
    assert avatar_from_fx(egg) is None
    assert avatar_from_fx({'code': 404, 'message': 'User not found'}) is None
    assert avatar_from_fx({}) is None and avatar_from_fx(None) is None


def test_is_x_avatar():
    assert is_x_avatar('https://pbs.twimg.com/profile_images/1/abc_400x400.jpg')
    assert not is_x_avatar('https://unavatar.io/twitter/drvolts')
    assert not is_x_avatar(None)
