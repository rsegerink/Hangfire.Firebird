Properties {
    $solution = "Hangfire.Firebird.sln"
}

Include "packages\Hangfire.Build.*\tools\psake-common.ps1"

Task Default -Depends Collect

Task Test -Depends Compile -Description "Run unit and integration tests." {
    Run-XunitTests "Hangfire.Firebird.Tests"
}

Task Merge -Depends Test -Description "Run ILMerge /internalize to merge assemblies." {
    # Remove `*.pdb` file to be able to prepare NuGet symbol packages.
    Remove-Item ((Get-SrcOutputDir "Hangfire.Firebird") + "\Dapper.pdb")

    Merge-Assembly "Hangfire.Firebird" @("Dapper")
}

Task Collect -Depends Merge -Description "Copy all artifacts to the build folder." {
    Collect-Assembly "Hangfire.Firebird" "Net45"

    Collect-Tool "src\Hangfire.Firebird\Install.v1.sql"
}

Task Pack -Depends Collect -Description "Create NuGet packages and archive files." {
    $version = Get-BuildVersion

    Create-Archive "Hangfire-Firebird-$version"
    Create-Package "Hangfire.Firebird" $version
}