::
::for %%f in ("%~1") do ( ffmpeg -i "%%f" -af "silenceremove=start_periods=1:start_threshold=-40dB:start_duration=0.05:stop_periods=1:stop_threshold=-40dB:stop_duration=0.05:detection=rms" -c:a libmp3lame -q:a 2 "%%~nf.mp3" )
for %%f in ("%~1") do ( ffmpeg -i "%%f" -c:a libmp3lame -q:a 2 "%%~nf.mp3" )
