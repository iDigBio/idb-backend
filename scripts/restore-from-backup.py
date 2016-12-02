from __future__ import division, absolute_import, print_function

from idb.helpers.logging import getLogger, configure_app_log
from idb.helpers.storage import IDigBioStorage
from idb.helpers.media_validation import sniff_mime
from idb.helpers.memoize import filecached
import hashlib

logger = getLogger("restore")
store = IDigBioStorage()


def get_object_from_backup(etag):
    "need to return a buffer that is the contents of the object that should be in `idigbio-images-prod/$etag`"
    obj = open('reestores/' + etag, 'rb').read()
    md5 = hashlib.md5()
    md5.update(obj)
    assert etag == md5.hexdigest()
    return obj


@filecached("/tmp/restore-from-backup.picklecache")
def get_fouled():
    """Return the original list, cached in a file that is written back as
    we make progress. This way as we make progrss and kill/rerun- the
    file it only tries new ones

    """
    return {(u'images', u'06fbc3c99d7d9f06e1487adbbe171f82', u'image/jpeg'),
            (u'images', u'0f4ceba6d970ad48e43e740a794b288a', u'image/jpeg'),
            (u'images', u'1155e9e1d4af33ebf3ab841523bb9a4c', None),
            (u'images', u'180ce69afc4dfed16f58dfa3da95adf4', u'image/jpeg'),
            (u'images', u'1f38a35fd0858ee5cd7899f2aace9b76', u'image/jpeg'),
            (u'images', u'1f9fb3722fc53835d00f4d1cd69dd3d8', u'image/jpeg'),
            (u'images', u'29c70c24dc2f886346feb584a49411e3', u'image/jpeg'),
            (u'images', u'3911efa02e46fcb5676e91c8e79d0dae', u'image/jpeg'),
            (u'images', u'399f1feaa3642fd4ed60c1130a2e9a5b', u'image/jpeg'),
            (u'images', u'39cbdea84c14b0592fd5233960151782', u'image/jpeg'),
            (u'images', u'3c9170d4cf7d9219d1d0c19e9b70fc48', u'image/jpeg'),
            (u'images', u'4e5bd8aba1751259b865298fa9ec0324', u'image/jpeg'),
            (u'images', u'5ac39a48987ab3f3c0dfc329e21443ef', u'image/jpeg'),
            (u'images', u'5f77dbe25c8570cf8e61e748ee918a36', u'image/jpeg'),
            (u'images', u'6469886c8f85fc0d98cd90fa6aabd993', u'image/jpeg'),
            (u'images', u'69ff96ea052a45b3159afc21afb25914', u'image/jpeg'),
            (u'images', u'6a351f9b36f119fee52d1d639b94e270', u'image/jpeg'),
            (u'images', u'73501c377300da9d91a9ed7e91f56375', u'image/jpeg'),
            (u'images', u'77031cacebcba231fdb19a74113edfa6', u'image/jpeg'),
            (u'images', u'80a0a5e574ff7befceda4217f21e9cc7', u'image/jpeg'),
            (u'images', u'8a372e3e42912b45c6f17e7432a587e4', u'image/jpeg'),
            (u'images', u'8fc85c02ed372aa0fb9a54b6e5f1b087', u'image/jpeg'),
            (u'images', u'999e9cab73ac76c0c843aa62f4cba0c5', u'image/jpeg'),
            (u'images', u'a9ac844a9a6c590770d52df6724ebcea', u'image/jpeg'),
            (u'images', u'ab4a31e8fcf79840be8b6f39f7493d1c', u'image/jpeg'),
            (u'images', u'b268032f9c2fe97d9580d5c097b6a829', u'image/jpeg'),
            (u'images', u'b652b66f4c80de3ea775215845c9f3ca', u'image/jpeg'),
            (u'images', u'b6ab472702fef4a4f527ad36579c79a5', u'image/jpeg'),
            (u'images', u'c63faf7478880d9e9ad41a4eaf22648f', u'image/jpeg'),
            (u'images', u'cb34bf6b7a108f19bfdf3c62cf675a3b', u'image/jpeg'),
            (u'images', u'ecedbfc39ed22a843934afc2353f86e5', u'image/jpeg'),
            (u'images', u'f1da30f47734ed6238a9817618815c69', u'image/jpeg'),
            (u'images', u'f3311755eb4f63bb7f9d1277080c7940', u'image/jpeg'),
            (u'images', u'fc0f15eef1a7cc33f8c9b4a695f6df36', u'image/jpeg')}


def restore_one(f):
    bucket, etag, mime = f
    try:
        logger.debug("%s attempting restore", etag)
        contents = get_object_from_backup(etag)
        k = store.get_key(etag, 'idigbio-images-prod')
        if mime is None:
            mime = sniff_mime(contents)
            logger.debug("%s detected mime of %s", etag, mime)
        k.set_metadata('Content-Type', mime)
        k.set_contents_from_string(contents, policy='public-read')
        logger.info("%s successfully restored", etag)
        return True
    except Exception:
        logger.exception("%s failed", etag)
        return False

def restore_all(fouled):
    for f in list(fouled):
        if restore_one(f):
            fouled.remove(f)
    return fouled


def kickstart():
    fouled = get_fouled()
    restore_all(fouled)
    logger.info("Finished restoring, %r didn't succeeed:", fouled)

if __name__ == '__main__':
    configure_app_log(2, journal='auto')
    kickstart()
