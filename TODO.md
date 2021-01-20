* Remove unwanted fmt::Debug / fmt::Display.
* Remove unwanted println!() macros.
* Review TODO marked code-blocks.
* mmaped index file.
* implement caching for intermediate-blocks, configurable based on depth.
* implement caching for zblocks.
* `_with_versions` for get(), range(), reverse(), iter() API. Try using NoDiff.
* Behaviour of bitmap index documentation.
  * deleted entries shall also be indexed in the bitmap.
