The Windows installer build contains its stream providers again. 3.0.0 packaged
none of the provider plugins, so the installed app showed "No providers" and
every search found nothing (#48). The release build now checks that the
providers load, and a build without them logs an error saying so.
