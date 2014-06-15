#!/usr/bin/env python
# coding:utf-8

from __future__ import with_statement

import os
import sys
import errno

from CoredataAPI import CoredataClient, Entity
from fuse import FUSE, Operations, FuseOSError


class Passthrough(Operations):
    def __init__(self):
        self.current_space = ''
        self.client = CoredataClient('<host>', ('<user>', '<pass>'))

    def access(self, path, mode):
        """
        Check file access permissions.
        """
        pass

    def chmod(self, path, mode):
        """
        Changing permissions of file/folder
        """
        pass

    def chown(self, path, uid, gid):
        """
        Changing ownership of file/folder
        """
        pass

    def getattr(self, path, fh=None):
        """
        Get file attributes.

        Due to how systems work, this needs to be implemented properly so the
        operating system can reason about the files on the system. ie. checking
        if files exists before trying to create them.
        """
        # TODO: Implement this properly
        title = os.path.basename(os.path.normpath(path))
        dirs = filter(lambda x: x != '', path.split('/'))
        entity = Entity.Projects
        if len(dirs) == 1:
            # We are on the first level so we are searching for a space
            entity = Entity.Spaces

        entities = self.client.find(entity, {'title': title})
        print title
        if not entities and title not in ['.', '..', '']:
            # Raise a FUSE error if we cannot find the requested path, this
            # gets caught in the super class and properly handled.
            raise FuseOSError(errno.ENOENT)
        print title
        return {
            'st_ctime': 1402489488.4108176, 'st_mtime': 1402489488.4108176,
            'st_nlink': 4, 'st_mode': 16893, 'st_size': 4096, 'st_gid': 1000,
            'st_uid': 1000, 'st_atime': 1402383556.9949517}

    def readdir(self, path, fh):
        """Read directory"""
        space_name = path[1:]
        import pdb; pdb.set_trace()
        if space_name != '':
            # Set the current space. Won't get set here unless user hits `ls`.
            # TODO: Fix this.
            space = self.client.find_one(
                Entity.Spaces, {'title': space_name})
            space_id = space['id']
            self.current_space = space_id
            docs = self.client.get(Entity.Spaces, space_id, Entity.Projects)
        else:
            docs = self.client.get(Entity.Spaces)
        return ['.', '..'] + map(lambda x: x['title'], docs['objects'])

    def readlink(self, path):
        pass

    def mknod(self, path, mode, dev):
        """
        Create a file node

        This is called for creation of all non-directory, non-symlink nodes.
        If the filesystem defines a create() method, then for regular files
        that will be called instead.
        """
        pass

    def rmdir(self, path):
        pass

    def mkdir(self, path, mode):
        """
        Create a directory
        """
        title = os.path.basename(os.path.normpath(path))

        space_name = filter(lambda x: x != '', path.split('/'))[0]
        if space_name:
            # Set the current space. Won't get set here unless user hits `ls`.
            # TODO: Fix this.
            space = self.client.find_one(
                Entity.Spaces, {'title': space_name})
            space_id = space['id']
            self.current_space = space_id

        print 'Trying to make {title} in space {space}. Path is {path}'.format(
            title=title, space=self.current_space, path=path)
        self.client.create(Entity.Projects, {'space': self.current_space,
                                             'title': title})

    def statfs(self, path):
        pass

    def unlink(self, path):
        pass

    def symlink(self, target, name):
        pass

    def rename(self, old, new):
        pass

    def link(self, target, name):
        """
        Create a hard link to a file
        """
        pass

    def utimens(self, path, times=None):
        """
        Change the access and modification times of a file with nanosecond
        resolution.

        This supersedes the old utime() interface. New applications should use
        this.
        """
        pass

    def open(self, path, flags):
        """
        File open operation

        No creation (O_CREAT, O_EXCL) and by default also no truncation
        (O_TRUNC) flags will be passed to open(). If an application specifies
        O_TRUNC, fuse first calls truncate() and then open(). Only if
        'atomic_o_trunc' has been specified and kernel version is 2.6.24 or
        later, O_TRUNC is passed on to open.

        Unless the 'default_permissions' mount option is given, open should
        check if the operation is permitted for the given flags. Optionally
        open may also return an arbitrary filehandle in the fuse_file_info
        structure, which will be passed to all file operations.
        """
        pass

    def create(self, path, mode, fi=None):
        """
        Create and open a file

        If the file does not exist, first create it with the specified mode,
        and then open it.
        """
        pass

    def read(self, path, length, offset, fh):
        """
        Read data from an open file

        Read should return exactly the number of bytes requested except on EOF
        or error, otherwise the rest of the data will be substituted with
        zeroes. An exception to this is when the 'direct_io' mount option is
        specified, in which case the return value of the read system call will
        reflect the return value of this operation.
        """
        pass

    def write(self, path, buf, offset, fh):
        pass

    def truncate(self, path, length, fh=None):
        pass

    def flush(self, path, fh):
        """
        Possibly flush cached data

        BIG NOTE: This is not equivalent to fsync(). It's not a request to sync
        dirty data.

        Flush is called on each close() of a file descriptor. So if a
        filesystem wants to return write errors in close() and the file has
        cached dirty data, this is a good place to write back data and return
        any errors. Since many applications ignore close() errors this is not
        always useful.
        """
        pass

    def release(self, path, fh):
        pass

    def fsync(self, path, fdatasync, fh):
        """
        Synchronize file contents

        If the datasync parameter is non-zero, then only the user data should
        be flushed, not the meta data.
        """
        pass


def main(mountpoint):
    FUSE(Passthrough(), mountpoint, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1])
