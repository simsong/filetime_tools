Notes and technology to turn a RaspberryPI into a digital picture frame.

# Goals
* Display photos in full-screen mode on a screen.
* Access to historical photos and current ones.
* Some intelligence is photo display --- for example, some sequential photos, then jump to someplace else and some sequential photos.
* Easy to set up and maintain.
* Turn off power when people aren't around

# Concept:
- Use a raspberry pi connected to a network and the screen.
- Display the photos with:

## Screen:
Have a cron script that turns the screen on and off, depending on time of day.
  --- Or we can just have it turn on every 5 minutes...

## Option 1
- a browser in full-screen mode.

References for full screen:
* https://developers.google.com/web/updates/2011/10/Let-Your-Content-Do-the-Talking-Fullscreen-API
* https://www.sitepoint.com/html5-full-screen-api/
* https://www.w3schools.com/howto/howto_js_fullscreen.asp

References for loading images:
* https://blog.teamtreehouse.com/using-jquery-asynchronously-loading-image


## Option 2;
- Option #2 - some sort of Python program
** Time of day
** If people are in the house.

```
sudo apt-get install python3-pil python3-pil.imagetk
```



# References
## Set up Raspberry PI
- https://www.raspberrypi.org/documentation/remote-access/ssh/
    sudo systemctl enable ssh
    sudo systemctl start ssh

Ssh doesn't allow access to an account without a password, so you will need to create a service account, because the pi account doesn't have a password:

- https://www.digitalocean.com/community/tutorials/how-to-create-a-sudo-user-on-ubuntu-quickstart
    sudo adduser simsong
    sudo usermod -aG sudo simsong


- Set up locals:

```
sudo localedef -i en_US -c -f UTF-8 en_US.UTF-8
sudo dpkg-reconfigure locales
```

Set /etc/default/locale to look like:
```
LANG=en_US.UTF-8
LC_ALL=en_US.UTF-8
LANGUAGE=en_US.UTF-8
```

    locale-gen en_US.UTF-8
    dpkg-reconfigure locale
    dpkg-reconfigure keyboard-configuration
    localedef -i en_US -c -f UTF-8 en_US.UTF-8
    reboot
    locale

## Accessing the display

Apparently all that's needed is setting the `DISPLAY` environment variable; no pipes or magic cookies:
    export DISPLAY=:0.0

## Monitor control:
* `xset dpms force off` --- turns off monitor
* `xset dpms force on` -- turns on monitor
* `xset dpms force suspend` - not sure what this does
* `xset dpms force standby` - not sure what this does
* `xset dpms [standby] [suspend] [off] -

References:
* https://raspberrypi.stackexchange.com/questions/22039/turning-on-hdmi-programmatically-doesnt-work

## Full-screen API
- https://stackoverflow.com/questions/7836204/chrome-fullscreen-api

## Display a JPEG on the screen
- (demo.py)[demo.py] - a small python program.
- Other options:
```feh --hide-pointer -x -q -B black -g 1280x800 /path/to/image/pic.jpg```
- https://raspberrypi.stackexchange.com/questions/18261/how-do-i-display-an-image-file-png-in-a-simple-window
- a program called `fbi` which can be installed with apt-get
- wand, and image-magick ctypes interface for python
```
import wand
with Image(blob=file_data) as image:
    wand.display.display(IMAGE)
```
then:
```
$ python -m wand.display wandtests/assets/mona-lisa.jpg
```


# Configuring MacOS
We use MacOS apache webserver installed through Macports

1. sudo port install apache
2. Modify `/opt/local/etc/apache2/httpd.conf` and uncomment this line:

```
# User home directories
Include etc/apache2/extra/httpd-userdir.conf
```

3. In `httpd.conf` uncomment:

```
LoadModule userdir_module lib/apache2/modules/mod_userdir.so
LoadModule cgi_module lib/apache2/modules/mod_cgi.so

    AddHandler cgi-script .cgi

```


3. Run this command:

```
sudo cp /opt/local/etc/apache2/extra/httpd-userdir.conf.orig /opt/local/etc/apache2/extra/httpd-userdir.conf
```

4. Modify `/opt/local/etc/apache2/extra/httpd-userdir.conf` and add ExecCGI to the Options line, so it reads:

```
<Directory "/Users/*/Sites">
    AllowOverride FileInfo AuthConfig Limit Indexes
    Options MultiViews Indexes SymLinksIfOwnerMatch IncludesNoExec ExecCGI
    Require method GET POST OPTIONS
</Directory>
```

5. It appears that your Apachedir is now /Users/$HOME/Sites/. Test by creating a file `hello.txt` and see if you can read it with `http://localhost/~$USER/hello.txt`


## Server
Perhaps I shoudl build something based on flask?


