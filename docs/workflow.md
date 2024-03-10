# workflow

initially it was conceptualized that nbac would just be triggered to run
as a local cron job and then store data to the filesystem.  while this can
certainly be supported, the data for nbac needs to be leveraged by larger
services so it is no longer a simple cli tool to download data, but a tool
that's being relied upon to synchronize large data sets.  the robustness
of the tool needs to be investigated to fit a larger use case.

## persistence

data needs to be persisted somewhere that's accessible outside of the 
local file system, a blob store will be used to store the data.

## triggering

nbac needs to be run once a day at least to pull data from the nba
and other other sources.  since this is run once a day or perhaps
re-triggered when an error occurs, this does not need to running
continuously in the background as this would be a waste of resources
for most of the time.  nbac can be scheduled once a day, ideally after
all boxscores for the day have been processed.

## progress and status

nbac sometimes encounters issues so the data is not always up to date,
upon pulling data again, nbac will resume from where it left off, so
typically re-running the tool will update all the missed data.  however,
from the prospective of a third party service, they will not know the
state of the data so some sort of scan of the data to check the state
needs to be done, otherwise queries to the downstream service maybe
outdated and extremely inconsistent.
