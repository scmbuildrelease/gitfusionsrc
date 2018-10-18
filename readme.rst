=======================================
Ravenbrook Customisations to Git Fusion
=======================================

:Author: Richard Brooksby <rb@ravenbrook.com>
:Organization: Ravenbrook Limited <http://www.ravenbrook.com/>
:Date: 2016-01-27
:Revision: $Id: //guest/richard_brooksby/ravenbrook-git-fusion/main/readme.rst#1 $

Ravenbrook is using the excellent Perforce Git Fusion
<https://www.perforce.com/perforce/doc.current/manuals/git-fusion/> to
maintain public Git repositories for our various open source projects.

As of version 2015.4, Git Fusion does not identify the Perforce Helix
server that a git commit comes from in the commit description.  We are
making a local change to Git Fusion to support this.  This repository
exists to version the change, and also share it with Perforce, so that they
can consider a compatible change.
