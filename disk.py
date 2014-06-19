#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

from __future__ import with_statement

import os
import sys
import errno
import config

from CoredataAPI import CoredataClient, Entity
from fuse import FUSE, Operations, FuseOSError
from tempfile import NamedTemporaryFile
from enum import Enum


class Utils:
    @staticmethod
    def split_path(path):
        path_items = filter(lambda x: x != '', path.split('/'))
        space, project, filename = (None, None, None)
        if len(path_items) == 0:
            # This be ugly
            space = None
        elif len(path_items) == 1:
            space = path_items[0]
        elif len(path_items) == 2:
            space, project = path_items
        elif len(path_items) == 3:
            space, project, filename = path_items
        else:
            print path_items
            raise Exception('Unsecsessful in splitting up local path')
        return (space, project, filename)


class CacheStatus(Enum):
    NotFound = 1
    File = 2
    Folder = 3


class CoredataDisk(Operations):
    def __init__(self, host, username, password):
        self.client = CoredataClient(host, (username, password))
        self.cache = {}
        self.file_cache = {}

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
        print 'Getting attributes for {path}'.format(path=path.encode('utf8'))
        if path == '/':
            return {
                'st_ctime': 1402489488.4108176, 'st_mtime': 1402489488.4108176,
                'st_nlink': 4, 'st_mode': 16893, 'st_size': 4096,
                'st_gid': 1000, 'st_uid': 1000, 'st_atime': 1402383556.9949517}

        space, project, filename = Utils.split_path(path)

        if path in self.file_cache:
            if self.file_cache[path] == CacheStatus.File:
                return {
                    'st_mode': 33204, 'st_ino': 4852076, 'st_dev': 36L,
                    'st_nlink': 1, 'st_uid': 1000, 'st_gid': 1000,
                    'st_size': 6905, 'st_atime': 1403002695,
                    'st_mtime': 1402520784, 'st_ctime': 1402520784}
            elif self.file_cache[path] == CacheStatus.Folder:
                return {
                    'st_ctime': 1402489488.4108176,
                    'st_mtime': 1402489488.4108176, 'st_nlink': 4,
                    'st_mode': 16893, 'st_size': 4096, 'st_gid': 1000,
                    'st_uid': 1000, 'st_atime': 1402383556.9949517}
            elif self.file_cache[path] == CacheStatus.NotFound:
                raise FuseOSError(errno.ENOENT)
            else:
                raise Exception('Cache is dirty with: {dirty}'.format(
                    self.file_cache[path]))

        if filename:
            entity = Entity.Files
            title = filename
        elif project:
            entity = Entity.Projects
            title = project
        elif space:
            entity = Entity.Spaces
            title = space
        else:
            print space, project, filename, path
            raise Exception('Failed figuring out entity type')

        # TODO: Cache this stuff.
        entities = self.client.get(entity, search_terms={'title': title})
        if not entities:
            # Raise a FUSE error if we cannot find the requested path, this
            # gets caught in the super class and properly handled.
            self.file_cache[path] = CacheStatus.NotFound
            raise FuseOSError(errno.ENOENT)
        if entity == Entity.Files:
            self.file_cache[path] = CacheStatus.File
            return {
                'st_mode': 33204, 'st_ino': 4852076, 'st_dev': 36L,
                'st_nlink': 1, 'st_uid': 1000, 'st_gid': 1000, 'st_size': 6905,
                'st_atime': 1403002695, 'st_mtime': 1402520784,
                'st_ctime': 1402520784}
        self.file_cache[path] = CacheStatus.Folder
        return {
            'st_ctime': 1402489488.4108176, 'st_mtime': 1402489488.4108176,
            'st_nlink': 4, 'st_mode': 16893, 'st_size': 4096, 'st_gid': 1000,
            'st_uid': 1000, 'st_atime': 1402383556.9949517}

    def readdir(self, path, fh):
        """Read directory"""
        print 'Reading directory {path}'.format(path=path.encode('utf8'))
        space, project, filename = Utils.split_path(path)

        if path in self.cache:
            return ['.', '..'] + self.cache[path]

        if not space:
            docs = self.client.get(Entity.Spaces)
            titles = map(lambda x: x['title'], docs)
        elif filename:
            raise NotImplementedError()
        elif project:
            project = self.client.get(Entity.Projects, limit=1, search_terms={
                'title': project})
            project_id = project[0]['id']
            docs = self.client.get(
                Entity.Projects, project_id, Entity.Files, limit=200)
            titles = map(lambda x: x['filename'], docs)
        elif space:
            space = self.client.get(
                Entity.Spaces, limit=1, search_terms={'title': space})
            space_id = space[0]['id']
            docs = self.client.get(
                Entity.Spaces, space_id, Entity.Projects, limit=200)
            titles = map(lambda x: x['title'], docs)

        self.cache[path] = titles
        return ['.', '..'] + titles

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
        space = self.client.find_one(Entity.Spaces, {'title': space_name})
        space_id = space['id']

        print 'Trying to make {title} in space {space}. Path is {path}'.format(
            title=title, space=space_id, path=path)
        self.client.create(Entity.Projects, {'space': space_id,
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
        """
        space, project, filename = Utils.split_path(path)
        # Get the file from the API, saving to temp file
        # Remove extension again
        title, _ = filename.split('.')
        # TODO: Make sure we only get one file back.
        f = self.client.get(Entity.Files, limit=1,
                            search_terms={'title': title})[0]
        file_id = f['id']

        # TODO: Add content in file endpoint and fetch it here.
        content = self.client.get(Entity.Files, file_id, Entity.Content)

        tmp_file = NamedTemporaryFile()
        tmp_file.write(content)

        # Open file descripton and pass the file descriptor
        print 'Returning tmp file!'
        return os.open(tmp_file.name, flags)

    def create(self, path, mode, fi=None):
        """
        Create and open a file

        If the file does not exist, first create it with the specified mode,
        and then open it.
        """
        print 'Trying to creating file: ' + path
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
        print 'Trying to read file: ' + path
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

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


if __name__ == '__main__':
    hostname = config.hostname
    username = config.username
    password = config.password
    disk = CoredataDisk(hostname, username, password)
    mountpoint = sys.argv[1]
    FUSE(disk, mountpoint, foreground=True)
