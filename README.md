# gis-scripts
This repo is for code that is useful and that I want to me able to reference online, but for which each individual script is not a part of a big enough project to warrant having it's own repo.  

The files themselves will be stored here and will have symlinks pointing to them in their respective project folders on my local machine or a network.  Using the Git Bash shell (aka mingw/msys) on Windows and a soft link can be created using the following command:

```
  cmd //c mklink .\\path\\to\\link.txt .\\path\\to\\target.txt
```

Note that the forward and back slashes have to be escaped here because this is really a Powershell command that is being executed in a Linux like environment.  Soft links are actually [broken](http://superuser.com/questions/893239/windows-symlink-to-executable-does-not-open-by-double-click) [right now](http://www.sevenforums.com/general-discussion/364618-windows-explorer-does-not-follow-symbolic-links.html) in Windows Explorer for Windows 7, but hard links work, which can be created by adding the /h switch:

```
  cmd //c mklink //h  .\\path\\to\\link.txt .\\path\\to\\target.txt
```
