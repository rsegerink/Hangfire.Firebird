Hangfire.Firebird
===================
This is an plugin to the Hangfire to enable Firebird as a storage system.
Read about hangfire here: https://github.com/HangfireIO/Hangfire#hangfire-
and here: http://hangfire.io/

Instructions
------------
Install Hangfire, see https://github.com/HangfireIO/Hangfire#installation

Download all files from this repository, add the Hangfire.Firebird.csproj to your solution.
Reference it in your project, and you are ready to go by using:

```csharp
app.UseHangfire(config =>
{
    config.UseFirebirdStorage("<connection string or its name>");
    config.UseServer();
});
```


Related Projects
-----------------

* [Hangfire.Core](https://github.com/HangfireIO/Hangfire)

License
--------

Copyright © 2015 Rob Segerink <http://discuss.hangfire.io/c/storage.

Hangfire.Firebird is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as 
published by the Free Software Foundation, either version 3 
of the License, or any later version.

Hangfire.Firebird  is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public 
License along with Hangfire.Firebird. If not, see <http://www.gnu.org/licenses/>.

This work is based on the work of Sergey Odinokov, author of 
Hangfire. <http://hangfire.io/>
  
   Special thanks goes to him.
