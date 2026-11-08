Motorsport series schedules (`motorsport_schedule.py`, Formula 1 first): every
session of the coming race weekends from ESPN's racing scoreboard, the current
or next session as a series channel's stream window, and the season from ESPN's
calendar. In the guide each session is its own programme, titled per session
kind ("F1 Qualifying", so Jellyfin can record it as a series) with the round as
its `<sub-title>` episode title ("São Paulo GP"), and Standby blocks between
sessions. The groundwork for a Formula 1 channel; nothing enables one yet.
Matching a series channel (`motorsport_terms.py`): it searches with the series'
names plus the current round's names and venues ("brazil gp", "interlagos"),
and rejects titles naming another series (F2, F3, F1 Academy, MotoGP, Moto2/3,
Formula E, IndyCar, NASCAR, ...) or another round of the series. Stream titles
are accent-folded before matching ("São Paulo" no longer becomes "s o paulo").
