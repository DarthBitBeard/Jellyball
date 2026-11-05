Jellyfin's ffmpeg is also found when Jellyfin is installed outside Program
Files (for example `C:\Jellyfin\Server`): Jellyball reads the folder from the
Jellyfin Server service. Third-party plugins in the installed app load from
the data folder's `plugins\third_party`, which upgrades keep.
