using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: AssemblyTitle("Hangfire.Firebird")]
[assembly: AssemblyDescription("Firebird job storage for Hangfire")]
[assembly: Guid("a5036b3d-7f95-48a5-b489-5d7b1ae673f8")]

[assembly: InternalsVisibleTo("Hangfire.Firebird.Tests")]
// Allow the generation of mocks for internal types
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]

[assembly: AssemblyProduct("Hangfire.Firebird")]
[assembly: AssemblyCompany("Rob Segerink")]
[assembly: AssemblyCopyright("Copyright © 2015 Rob Segerink")]
[assembly: AssemblyCulture("")]

[assembly: ComVisible(false)]
[assembly: CLSCompliant(false)]

// Don't edit manually! Use `build.bat version` command instead!
[assembly: AssemblyVersion("1.3.4")]
[assembly: AssemblyInformationalVersion("1.3.4")]
[assembly: AssemblyFileVersion("1.0.0.0")]