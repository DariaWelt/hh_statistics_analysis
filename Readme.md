requirements:
dotnet 6.0

compile and start LazyExtractor:
```
cd DataExtractor
dotnet build LazyExtractorService --output ./build_output    
dotnet ./build_output/LazyExtractorService.dll --LazyExtractor:workers=5
```
