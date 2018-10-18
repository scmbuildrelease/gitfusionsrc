#! /usr/bin/env python3.3
""" Constants for Perforce schema.

    http://www.perforce.com/perforce/r14.2/schema/
"""

from p4gf_hex_str import md5_str

                        # Some day we'll support Python 3.4 and get its shiny
                        # new Enum support. Until then, here have a fake
                        # replacement.
try:
    from enum import Enum
except ImportError:
    class Enum:
        """Gee golly I wish we had Python 3.4 Enum."""
        def __init__(self):
            pass

# Permit Royal Canterlot Voice for all constants.
# pylaint:disable=invalid-name

class ChangeStatus(Enum):
    """ChangeStatus enum for db.change.status."""
    PENDING              =  0
    COMMITTED            =  1
    SHELVED              =  2
    RESTRICTED_PENDING   =  4
    RESTRICTED_COMMITTED =  5
    HIDDEN_SHELF_ONLY    =  8
    PROMOTED_SHELF_ONLY  = 16


class FileType(Enum):
    """Perforce file type."""
                                    # Bits used to store the server's
                                    # storage type
                                    #
    S_MASK          = 0x0000000F    # mask for the bits below
    S_RCS           = 0x00000000    # *,v single text file with RCS deltas
    S_BINARY        = 0x00000001    # */   dir of individual raw file revs
    S_TINY          = 0x00000002    # tiny.db records
    S_GZ            = 0x00000003    # *,d/ dir of individual gz file revs


                                    # dmtypes.h DmtFileClientType
                                    #
                                    # Bits used to store the file type from
                                    # the client's point of view.
                                    #
    MASK            = 0x010D0000    # mask
    TEXT            = 0x00000000    # text
    BINARY          = 0x00010000    # binary
    EXECUTABLE      = 0x00020000    # executable bit set
    SYMLINK         = 0x00040000    # symlink
    RESOURCE_FORK   = 0x00050000    # resource fork
    UNICODE         = 0x00080000    # unicode
    RAWTEXT         = 0x00090000    # raw text (nocrlf - 99.1)
    APPLE_2000      = 0x000C0000    # apple data + resource file (2000.2+)
    APPLE_1999      = 0x000D0000    # apple data + resource file (99.2)
    DETECT          = 0x01000000    # Used to support filetype detection

                                    # ubinary = binary+F
                                    # binary: Reproduce verbatim with no line-
                                    #         ending conversion.
                                    # +F    : Store full file per revision,
                                    #         verbatim. Do not compress.
                                    # Used for tree and commit objects which
                                    # we already compress.
    UBINARY         = BINARY + S_BINARY

                                    # ctext = text+C
                                    # text : Yeah, it's a text file that can
                                    #        safely be stored as utf-8.
                                    # +C   : Store compressed file per
                                    #        revision.
                                    # Used for depot branch-info and config
                                    # files.
    CTEXT           = TEXT   + S_GZ


                        # Git Fusion creates only these three base filetypes.
                        # Git Fusion uses "binary" for anything else,
                        # _especially_ unicode files, which will suffer from
                        # BOM bytes stripped/inserted if we store as a unicode
                        # format.
    SUPPORTED_P4FILETYPES = {
          "text"    : TEXT
        , "binary"  : BINARY
        , "symlink" : SYMLINK
        }

    SUPPORTED_P4FILETYPES_R = { v : k  for k, v in SUPPORTED_P4FILETYPES.items() }
    SUPPORTED_P4FILETYPES_MASK = sum(v for v in SUPPORTED_P4FILETYPES.values())

                        # Convert all known p4filetype modifiers to bits.
    SUPPORTED_P4FILETYPE_MODS = {
          "m"    : 0x00200000 # always set modtime on client
        , "w"    : 0x00100000 # always writable on client
        , "x"    : EXECUTABLE # exec bit set on client
        , "k"    : 0x00000020 # RCS keyword expansion
        , "ko"   : 0x00000010 # RCS keyword expansion of ID and Header only
        , "l"    : 0x00000040 # exclusive open: disallow multiple opens
        , "C"    : 0x00000003 # server stores compressed file per revision
        , "D"    : 0x00000000 # server stores deltas in RCS format
        , "F"    : 0x00000001 # server stores full file per revision
        , "X"    : 0x00000008 # server runs archive trigger to access files
        , "S"    : 0x00000080 # server stores only single head revision
        , "S1"   : 0x00000080
        , "S2"   : 0x00000180
        , "S3"   : 0x00000280
        , "S4"   : 0x00000380
        , "S5"   : 0x00000480
        , "S6"   : 0x00000580
        , "S7"   : 0x00000680
        , "S8"   : 0x00000780
        , "S9"   : 0x00000880
        , "S10"  : 0x00000980
        , "S16"  : 0x00000A80
        , "S32"  : 0x00000B80
        , "S64"  : 0x00000C80
        , "S128" : 0x00000D80
        , "S256" : 0x00000E80
        , "S512" : 0x00000F80
        }

    @staticmethod
    def from_base_mods(base_mods):
        """Convert ["text", "x"] to TEXT + EXECUTABLE bits.

        Only works for supported filetypes and mods.

        See p4gf_p4filetype.to_base_mods() for base_mods conversion.
        """
        bits = 0
        assert base_mods[0] in FileType.SUPPORTED_P4FILETYPES
        bits = FileType.SUPPORTED_P4FILETYPES[base_mods[0]]
        for mod in base_mods[1:]:
                        # pylint:disable=no-member
                        # Instance of 'SUPPORTED_P4FILETYPE_MODS' has no 'get' member
                        # Yes it does. Pylint is incorrect here.
            bits += FileType.SUPPORTED_P4FILETYPE_MODS.get(mod, 0)
        return bits

    @staticmethod
    def to_base(bits):
        """Convert TEXT to "text" """
        return FileType.SUPPORTED_P4FILETYPES_R[
                    FileType.SUPPORTED_P4FILETYPES_MASK & bits ]

class FileAction(Enum):
    """Revisions are produced by user actions.
    The Action type defines the available actions and their internal values.
    """
    ADD             = 0             # add; user adds a file
    EDIT            = 1             # edit; user edits a file
    DELETE          = 2             # delete; user deletes a file
    BRANCH          = 3             # branch; add via integration
    INTEG           = 4             # integ; edit via integration
    IMPORT          = 5             # import; add via remote depot
    PURGE           = 6             # purged revision, no longer available
    MOVE_FROM       = 7             # movefrom; move from another filename
    MOVE_TO         = 8             # moveto; move to another filename
    ARCHIVE         = 9             # archive; stored in archive depot


class IntegHow(Enum):
    """Integration methods

    Specifies how an integration was performed. Integrations always come in
    pairs: the 'forward' and 'reverse' records. As a general rule, the forward
    records contain the word 'from'; and the reverse records contain the word
    'into'.

    Note: All integration records should have a corresponding reverse record.
    The 'branch from' (2) and 'merge from' (0) records have corresponding
    "dirty" reverse records for cases where the target file took more than one
    change or was (re)opened for 'edit' or 'add'.

    Reverse actions here have a _R suffix.
    """
    MERGE           =  0  # merge from: integration with other changes
    MERGE_R         =  1  # merge into: reverse merge
    BRANCH          =  2  # branch from: integration was branch of file
    BRANCH_R        =  3  # branch into: reverse branch
    COPY            =  4  # copy from: integration took source file whole
    COPY_R          =  5  # copy into: reverse take
    IGNORE          =  6  # ignored: integration ignored source changes
    IGNORE_R        =  7  # ignored by: reverse copy
    DELETE          =  8  # delete from: integration of delete
    DELETE_R        =  9  # delete into: reverse delete
    MERGE_EDIT_R    = 10  # edit into: reverse of integ downgraded to edit
    BRANCH_EDIT_R   = 11  # add into: reverse of branch downgraded to add
    MERGE_EDIT      = 12  # edit from: merge that the user edited
    BRANCH_EDIT     = 13  # add from: branch downgraded to add
    MOVE_DELETE     = 14  # moved from: reverse of renamed file
    MOVE_ADD        = 15  # moved into: file was renamed
    IGNORE_DELETE   = 16  # 'delete' target rev ignoring non-deleted source
    IGNORE_DELETE_R = 17  # non-deleted source ignored by 'delete' target rev
    IGNORE_INTEG    = 18  # 'integrate' target rev ignoring deleted source
    IGNORE_INTEG_R  = 19  # deleted source ignored by 'integrate' target rev

class TraitLot(Enum):
    """The id of a group of traits

    An integer identifying a group of traits which may be shared by
    several revisions.

    Git Fusion does not use traits.
    """
    NONE = 0

class RevStatus(Enum):
    """A revision status

    Lazy copy, task stream promotion, and charset status
    """
    NOT_LAZY        = 0x0000  # single revision with content
    LAZY            = 0x0001  # single revision lazy copied
    TASK_SPARSE     = 0x0002  # revision task sparse
    TASK_BRANCH     = 0x0004  # revision task branch


def db_change( *
             , change_num
             , client
             , user
             , date
             , status      = ChangeStatus.COMMITTED
             , description
             , root        = "//..."
             ):
    """Return a db.change journal record as a single string.

    db.change - Changelists

    change      Change          The change number
    descKey     Change          The description key. Normally the same as
                                    'change', but may differ if a changelist
                                    was renumbered on submission.
    client      Domain          The client from which the change originates
    user        User            The user who owns the change
    date        Date            Date and time the changelist was submitted
                                As integer seconds since the epoch, in the
                                server's timezone.
    status      ChangeStatus    Status of the change
    description DescShort       Short description of the change.
    root        Mapping         Common path for all files in the changelist
    """
    short_desc = description[:40]
    return " ".join( ["@pv@ 2 @db.change@"
                     , str(change_num)        # change
                     , str(change_num)        # descKey
                     , at(client)             # client
                     , at(user)               # user
                     , str(date)              # date
                     , str(status)            # status
                     , at_esc(short_desc)     # description
                     , at(root)               # root
                     ])

def db_desc( *
           , change_num
           , description
           ):
    """Return a db.desc journal record as a single string.

    db.desc - Change descriptions

    descKey     Change  Original number of the change to which this
                            description applies. This may differ from the
                            final change number if the change is renumbered
                            on submission.
    description Text    The change description itself
    """
    return " ".join( ["@pv@ 0 @db.desc@"
                     , str(change_num)         # descKey
                     , at_esc(description)     # description
                     ])


def db_rev( *
          , depot_path
          , depot_rev
          , depot_file_type_bits
          , file_action_bits
          , change_num
          , date
          , md5
          , uncompressed_byte_ct
          , lbr_is_lazy
          , lbr_path
          , lbr_rev
          , lbr_file_type_bits
          ):
    """Return a db.rev jounal record as a single string.

    db.rev - Revision records

    depotFile   File        The file name
    depotRev    Rev         The revision number
    type        FileType    The file type of the revision
    action      Action      The action that created the revision
    change      Change      The changelist that created the revision
    date        Date        The date/time the changelist that created
                                the revision was submitted
                                ZZ adds: integer seconds since the epoch,
                                in Perforce server time zone.
    modTime     Date        The timestamp on the file in the user's
                                workspace when the revision was submitted.
                                ZZ adds: integer seconds since the epoch,
                                in Perforce server time zone.
    digest      Digest      The MD5 digest of the revision.
    size        FileSize    The size of the file in bytes
    traitLot    TraitLot    Group of traits (attributes) associated with
                                the revision.
    lbrIsLazy   RevStatus   Specifies whether or not the revision gets
                                its content from another file (lazy copy),
                                data about promotion of revision content
                                from task streams, and data about revision
                                charset information.
    lbrFile     File        Filename for librarian's purposes. Specifies
                                location in the archives where the file
                                containing the revision may be found.
                                Usually the same as depotFile, but differs
                                in the case of branched/copied revisions.
    lbrRev      String      The revision of lbrFile that contains the
                                revision content.
    lbrType     FileType    The file type of the librarian revision.
                                Usually the same as type, but differs in
                                the case of branched/copied revisions.
    """
    return " ".join( ["@pv@ 9 @db.rev@"
                     , at(depot_path)                 # depotFile
                     , str(depot_rev)                 # depotRev
                     , str(depot_file_type_bits)      # type
                     , str(file_action_bits)          # action
                     , str(change_num)                # change
                     , str(date)                      # date
                     , str(date)                      # modTime
                     , md5_str(md5)                   # digest
                     , str(uncompressed_byte_ct)      # size
                     , str(TraitLot.NONE)             # traitLot
                     , str(lbr_is_lazy)               # lbrIsLazy
                     , at(lbr_path)                   # lbrFile
                     , at(lbr_rev)                    # lbrRev
                     , str(lbr_file_type_bits)        # lbrType
                     ] )

class DbRev:
    """All the fields to fill in a db.rev record, as a struct that you
    can store or pass around.

    Constructor takes the exact same inputs as db_rev().
    """
    def __init__(self, *
          , depot_path
          , depot_rev
          , depot_file_type_bits
          , file_action_bits
          , change_num
          , date_p4d_secs
          , md5
          , uncompressed_byte_ct
          , lbr_is_lazy
          , lbr_path
          , lbr_rev
          , lbr_file_type_bits
          ):
        self.depot_path             = depot_path
        self.depot_rev              = depot_rev
        self.depot_file_type_bits   = depot_file_type_bits
        self.file_action_bits       = file_action_bits
        self.change_num             = change_num
        self.date_p4d_secs          = date_p4d_secs
        self.md5                    = md5
        self.uncompressed_byte_ct   = uncompressed_byte_ct
        self.lbr_is_lazy            = lbr_is_lazy
        self.lbr_path               = lbr_path
        self.lbr_rev                = lbr_rev
        self.lbr_file_type_bits     = lbr_file_type_bits

    def __str__(self):
        """Return our db.rev record as a string."""
        return db_rev(
              depot_path             = self.depot_path
            , depot_rev              = self.depot_rev
            , depot_file_type_bits   = self.depot_file_type_bits
            , file_action_bits       = self.file_action_bits
            , change_num             = self.change_num
            , date                   = self.date_p4d_secs
            , md5                    = self.md5
            , uncompressed_byte_ct   = self.uncompressed_byte_ct
            , lbr_is_lazy            = self.lbr_is_lazy
            , lbr_path               = self.lbr_path
            , lbr_rev                = self.lbr_rev
            , lbr_file_type_bits     = self.lbr_file_type_bits
            )

# end class DbRev
# ----------------------------------------------------------------------------


def db_integed_pair( *
    , src_depot_path
    , src_start_rev_int
    , src_end_rev_int
    , dest_depot_path
    , dest_rev_int
    , how
    , how_r
    , dest_change_num_int
    ):
    """Return a PAIR of db.integed journal records as a pair of strings.

    db.integed

    toFile        File      File to which integ is being performed (target).
    fromFile      File      File from which integ is being performed (source).
    startFromRev  Rev       Starting revision of fromFile
    endFromRev    Rev       Ending revision of fromFile
    startToRev    Rev       Start revision of toFile into which integration
                              was performed.
    endToRev      Rev       End revision of toFile into which integration
                              was performed. Only varies from startToRev
                              for reverse integration records.
    how           IntegHow  Integration method: variations on
                              merge/branch/copy/ignore/delete.
    change        Change    Changelist associated with the integration.
    """
    integ_jnl = " ".join(   ["@pv@ 0 @db.integed@"
                            , at(dest_depot_path)           # toFile
                            , at(src_depot_path)            # fromFile
                            , str(src_start_rev_int - 1)    # startFromRev
                            , str(src_end_rev_int)          # endFromRev
                            , str(dest_rev_int - 1)         # startToRev
                            , str(dest_rev_int)             # endToRev
                            , str(how)                      # how
                            , str(dest_change_num_int)      # change
                            ])
    integ_jnl_r = " ".join( ["@pv@ 0 @db.integed@"
                            , at(src_depot_path)            # toFile
                            , at(dest_depot_path)           # fromFile
                            , str(dest_rev_int - 1)         # startFromRev
                            , str(dest_rev_int)             # endFromRev
                            , str(src_start_rev_int - 1)    # startToRev
                            , str(src_end_rev_int)          # endToRev
                            , str(how_r)                    # how
                            , str(dest_change_num_int)      # change
                            ])
    return (integ_jnl, integ_jnl_r)


def at_esc(text):
    """Perforce journal files use "@" as a string delimiter. Escape all
    "@" chars by doubling them to "@@".
    """
    return "@" + text.replace("@","@@") + "@"


def at(text):           # pylint:disable=invalid-name
    """Wrap Perforce journal strings with "@" delimiters."""
    return "@" + text + "@"
