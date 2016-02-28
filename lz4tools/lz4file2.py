#!/usr/bin/python

import lz4f
import struct

class Lz4File:
    LZ4IO_MAGICNUMBER = 0x184D2204
    LZ4IO_SKIPPABLE0 = 0x184D2A50
    LZ4IO_SKIPPABLEMASK = 0xFFFFFFF0
    LEGACY_MAGICNUMBER = 0x184C2102

    def __init__(self, name, mode):
        self.__bRead = False
        self.__bWrite = False

        if 'r' in mode:
            self.__bRead = True
        if 'w' in mode:
            self.__bWrite = True
        if 'a' in mode:
            self.__bWrite = True
        if '+' in mode:
            raise IOError('Both write and read not supported')
        if 'b' not in mode:
            mode += 'b'

        self.__name = name
        self.__mode = mode
        self.__file = open(name, mode)
        self.__bClose = False

        self.__frames = []
        self.__iFrame = -1
        self.__block = None
        self.__pos = 0
        self.__ctx = lz4f.createDecompContext()

        self.__blockdata = ''
        self.__blockpos = 0

    @property
    def name(self):
        return self.__name
    @property
    def file(self):
        return self.__file
    @property
    def pos(self):
        return self.__pos

    def read(self, size=None):
        if not self.__bRead:
            raise IOError('File not open for reading')

        result = []
        cpt = 0

        while size is None or cpt < size:

            # First try to read data in the current loaded block
            if self.__blockpos < len(self.__blockdata):
                #print "Read in current block"
                toRead = len(self.__blockdata) - self.__blockpos
                if size is not None:
                    toRead = min(size - cpt,  toRead)

                rawdata = self.__blockdata[self.__blockpos:self.__blockpos + toRead]
                cpt += toRead
                self.__blockpos += toRead
                result.append(rawdata)

            # Second try to load the next block from the current frame
            elif self.__iFrame >= 0 and self.__iFrame < len(self.__frames) and not self.__frames[self.__iFrame].empty:
                #print "Load a new block"
                self.__loadBlock()

            # Finally try to load the next frame
            else:
                #print "Load a new frame"
                self.__loadFrame()

        self.__pos += cpt

        return ''.join(result)

    def write(self, size=None):
        if not self.__bRead:
            raise IOError('File not open for writing')

        raise NotImplementedError();

    def seek(self, offset, whence=0):
        raise NotImplementedError();

    def close(self):
        if not self.__bClose:
            lz4f.freeDecompContext(self.__ctx)
            self.file.close()
            self.__ctx = None
            self.__file = None
            self.__bClose = True

        raise NotImplementedError();

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __readFrame(self, pos=None):
        if pos is not None:
            savepos = self.file.tell()
            self.file.seek(pos, 0)

        while True:
            # Read magic number
            raw = self.file.read(4)

            # EOF ?
            if len(raw) == 0:
                return None

            # Check magic size
            if len(raw) != 4:
                raise IOError('File has been truncated')

            magic = struct.unpack('<I', raw)[0]

            if (magic & Lz4File.LZ4IO_SKIPPABLEMASK) == Lz4File.LZ4IO_SKIPPABLE0:
                magic = Lz4File.LZ4IO_SKIPPABLE0

            if magic == Lz4File.LZ4IO_MAGICNUMBER:
                # Read frame descriptor : 3 to 11 bytes
                raw = self.file.read(11)
                # Don't forget to back in file, because decompressor expect header and descriptor bytes
                self.file.seek(-(4 + len(raw)), 1)
                return Lz4Frame(self, raw, self.file.tell())

            elif magic == Lz4File.LEGACY_MAGICNUMBER:
                raise NotImplementedError('Legacy frame not supported')

            elif magic == Lz4File.LZ4IO_SKIPPABLE0:
                raw = self.file.read(4)

                if len(raw) != 4:
                    raise IOError('File has been truncated')

                sizeToSkip = struct.unpack('<I', raw)[0]
                self.file.seek(sizeToSkip, 1)

            else:
                print raw
                raise IOError('Not a valid LZ4 frame')

        if pos is not None:
            self.file.seek(savepos, 0)

    def __loadFrame(self):
        self.__iFrame += 1
        self.__blockdata = ''
        self.__blockpos = 0

        # if new index reaches the end of frame array, try to read new one
        if self.__iFrame >= len(self.__frames):
            frame = self.__readFrame()

            # No more frame ?
            if frame is None:
                break

            self.__frames.append(frame)

        # Use an already discovered frame
        else:
            frame = self.__frames[self.__iFrame]

        # Init decompression context with this frame header
        raw = self.file.read(frame.szHeader)
        res = lz4f.decompressFrame(raw, self.__ctx)
        if len(res['decomp']) != 0:
            raise IOError('Unexpected output')

    def __loadBlock(self):
        frame = self.__frames[self.__iFrame]

        # Read block's size
        raw = self.file.read(4)

        if len(raw) != 4:
            raise IOError('LZ4 block has been truncated')

        sizeBlock = struct.unpack('<I', raw)[0]
        sizeBlock &= 0x7fffffff

        # EndMark reached
        #  - push end of mark block and content checksum in decompressor engine (maybe useless ...)
        #  - mark current frame as empty
        if sizeBlock == 0:
            res = lz4f.decompressFrame(raw, self.__ctx)
            #print "  next={0}".format(res['next'])
            if len(res['decomp']) != 0:
                raise IOError('Unexpected output')

            # Read content checksum if enabled
            if frame.bContentChecksum:
                raw = self.file.read(4)
                if len(raw) != 4:
                    raise IOError('Not enought data for content checksum')

                # TODO check content checksum
                res = lz4f.decompressFrame(raw, self.__ctx)
                #print "  next={0}".format(res['next'])
                if len(res['decomp']) != 0:
                    raise IOError('Unexpected output')

            frame.empty = True

        # Another block to process, go back in file 
        else:
            self.file.seek(-4, 1)

            toRead = 4 + sizeBlock + (4 if frame.bBlockChecksum else 0)
            raw = self.file.read(toRead)
            if(len(raw) != toRead):
                raise IOError('LZ4 block has been truncated')

            res = lz4f.decompressFrame(raw, self.__ctx)
            #print "  next={0}".format(res['next'])
            self.__blockdata = res['decomp']
            self.__blockpos = 0

class Lz4Frame:
    TABBLOCKSIZE = [64 * 1024, 256 * 1024, 1 * 1024 * 1024, 4 * 1024 * 1024]

    def __init__(self, lz4file, rawDesriptor, filepos):
        self.__lz4file = lz4file
        self.__filepos = filepos
        self.empty = False

        if len(rawDesriptor) < 3:
            raise IOError('LZ4 frame has been truncated')

        (FLG, BD) = struct.unpack('BB',rawDesriptor[0:2])

        self.__version = FLG >> 6
        self.__bBlockIndep = ((FLG >> 5) & 1) != 0
        self.__bBlockChecksum = ((FLG >> 4) & 1) != 0
        self.__bContentSize = ((FLG >> 3) & 1) != 0
        self.__bContentChecksum = ((FLG >> 2) & 1) != 0
        self.__blockSize = (BD >> 4) & 7
        self.__contentSize = None

        if self.version != 1:
            raise IOError('This version of LZ4 frame is not supported')

        if self.blockSize < 4 or self.blockSize > 7:
            raise IOError('Invalid block size')

        self.__blockSizeBytes = Lz4Frame.TABBLOCKSIZE[self.blockSize - 4]

        if self.bContentSize:
            if len(rawDesriptor) < 11:
                raise IOError('LZ4 frame has been truncated')

            self.__contentSize = struct.unpack('<Q', rawDesriptor[2:10])[0]

        self.__frameChecksum = struct.unpack('B',rawDesriptor[-1])[0]
        #TODO : check frameChecksum

        self.__szHeader = 4 + 3 + (8 if self.bContentSize else 0)

    @property
    def lz4file(self):
        return self.__lz4file     
    @property
    def filepos(self):
        return self.__filepos
    @property
    def version(self):
        return self.__version
    @property
    def bBlockIndep(self):
        return self.__bBlockIndep
    @property
    def bBlockChecksum(self):
        return self.__bBlockChecksum
    @property
    def bContentSize(self):
        return self.__bContentSize
    @property
    def bContentChecksum(self):
        return self.__bContentChecksum
    @property
    def blockSize(self):
        return self.__blockSize
    @property
    def contentSize(self):
        return self.__contentSize
    @property
    def frameChecksum(self):
        return self.__frameChecksum
    @property
    def blockSizeBytes(self):
        return self.__blockSizeBytes
    @property
    def szHeader(self):
        return self.__szHeader

    def print_blocks(self):
        f = self.lz4file.file

        savepos = f.tell()
        f.seek(self.filepos + self.szHeader, 0)

        while True:
            raw = f.read(4)
            if len(raw) != 4:
                raise IOError('LZ4 block has been truncated')

            sizeBlock = struct.unpack('<I', raw)[0]
            sizeBlock &= 0x7fffffff

            if sizeBlock == 0:
                break

            print 'pos={0} size={1}'.format(f.tell() , sizeBlock)

            f.seek(sizeBlock + (4 if self.bBlockChecksum else 0) ,1)

        f.seek(savepos, 0)

if __name__ == '__main__':
   import sys

   f = Lz4File(sys.argv[1], "r")
