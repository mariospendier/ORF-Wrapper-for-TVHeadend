# ORF-Wrapper-for-TVHeadend
This is a wrapper for ORF2 Channels in TVHeadend. They stop working after a national - regional - national switch. This wrapper takes VLC as a video stream "cleaner", so the stream can be taken for MUX for new ORF2 channels, which work.

I use VLC player in the background because it is very stable within PMT changes, absolutely no freezes when ORF2 channels come to another regional stream. player is started and ended by a python wrapper. This wrapper runs on port 8800. Save it and start it as systemd or on command line (for testing). Touch Log file (/var/log/orf_vlc_wrapper.log) and add user (orfwrap, ...).

Save files in supposed locations:
- Wrapper file (ORF VLC Wrapper (Important! Correct the service IDs according to your TVH installation!!). Run it under the new user you added (there are several dependencies, so stay calm;-))
- Systemd File

systemctl daemon-reload

systemctl enable --now orf_vlc_wrapper

systemctl restart orf_vlc_wrapper


The result is a wrapper server which gives you an easy and flat stream. I do not recode, so your CPU should be ok. Take your servername or IP:

http://tvheadend.lan:8800/orf2b.ts

http://tvheadend.lan:8800/orf2w.ts

http://tvheadend.lan:8800/orf2o.ts

http://tvheadend.lan:8800/orf2k.ts

http://tvheadend.lan:8800/orf2n.ts

http://tvheadend.lan:8800/orf2st.ts

http://tvheadend.lan:8800/orf2s.ts

http://tvheadend.lan:8800/orf2t.ts

http://tvheadend.lan:8800/orf2v.ts


Use theses URLs to add 9 IPTV Muxes in TVH. After a scan you should have one stream within every MUX.


Rename the Astra channels so that you can make new Channels, with the original names. 
Add the new loopback services as service. 
Mind the EPG - just use the Astra EPG, works! 
Add Logo.


Drink beer now!
