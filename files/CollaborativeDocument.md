![](https://i.imgur.com/iywjz8s.png)


# Collaborative Document

2023-05-15 Interactive Geospatial Data Analysis at HPC Scales: Introduction to RS-DAT.

Welcome to The Workshop Collaborative Document.

This Document is synchronized as you type, so that everyone viewing this page sees the same text. This allows you to collaborate seamlessly on documents.

----------------------------------------------------------------------------

This is the Document for today: https://tinyurl.com/2023-05-15-rs-dat


## 👮Code of Conduct

Participants are expected to follow these guidelines:
* Use welcoming and inclusive language.
* Be respectful of different viewpoints and experiences.
* Gracefully accept constructive criticism.
* Focus on what is best for the community.
* Show courtesy and respect towards other community members.
 
 
## ⚖️ License

All content is publicly available under the Creative Commons Attribution License: [creativecommons.org/licenses/by/4.0/](https://creativecommons.org/licenses/by/4.0/).

## 🙋Getting help

To ask a question, just raise your hand.

If you need help from a helper, place a pink post-it note on your laptop lid. A helper will come to assist you as soon as possible.

## 🖥 Workshop website

[link](https://www.itc.nl/research/research-facilities/labs-resources/itc-big-geodata/training/introduction-to-rs-dat/)

## 🛠 Setup

1.  **Install Python**  (recommended via [Anaconda](https://www.anaconda.com/download)). Detailed installation instructions for various operating systems can be found on [The Carpentries template website for workshops](https://carpentries.github.io/workshop-template/#python) and in  [Anaconda documentation](https://docs.anaconda.com/anaconda/install).

    :::spoiler *Verify Python is installed*
    In the Terminal/Anaconda Prompt, typing:
    ```shell
    python --version
    ```
    should return (the version could actually differ):
    ```shell
    Python 3.9.15
    ```
    :::

2.  **Setup SSH key for Spider**:
    * Download the key using the link on the printout; 
    * On *Linux/Mac OS* change the key file permissions to only user can read/write by typing the following command in the Terminal/Anaconda Prompt (replace `XX` with your student number): 
        ```shell
        chmod 600 rsa_stursdat_XX
        ```
      
    :::spoiler *Verify SSH key is set*
    
    In the Terminal/Anaconda Prompt, typing (replace `XX` with your student number):
    ```shell
    ssh -i rsa_stursdat_XX stursdat-XX@spider.surf.nl "python --version"
    ```
    should return:
    ```shell
    Python 3.10.10
    ```
    :::
    
3.  **Setup scripts to access Spider**:
    * Download and extract the following zip archive: [link](https://github.com/RS-DAT/JupyterDaskOnSLURM/archive/refs/heads/workshops.zip) 
    * Using the Terminal/Anaconda Prompt, access the `JupyterDaskOnSLURM-workshops` folder and install `fabric`:
      ```shell
      cd JupyterDaskOnSLURM-workshops ; pip install fabric
      ```

    :::spoiler *Verify scripts are available*
    
    In the Terminal/Anaconda Prompt, typing:
    ```shell
    python runJupyterDaskOnSLURM.py 
    ```
    should return:
    ```
    usage: runJupyterDaskOnSLURM.py [-h] [--local_port LOCAL_PORT] [--wait_time WAIT_TIME] (--add_platform | --one_off | --uid UID) [--mode MODE]
    runJupyterDaskOnSLURM.py: error: one of the arguments --add_platform/-a --one_off/-oo --uid/-u is required
    ```
    :::

## 👩‍🏫👩‍💻🎓 Instructors

Meiert W. Grootes, Francesco Nattino, Pranav Chandramouli, Lodewijk Nauta

## 🧑‍🙋 Helpers

Serkan Girgin

## 🗓️ Agenda

 
| Time  | Topic                                       |
| ----- | ------------------------------------------- |
| 09:30 | Welcome and icebreaker                      |
| 09:45 | SURF services for research and SPIDER       |
| 10:00 | HPC, RS-DAT, and the EO software ecosystem  |
| 11:00 | *Coffee break*                              |
| 11:15 | Deployment with RS-DAT and data retrieval   |
| 12:30 | *Lunch break*                               |
| 13:30 | Scaling EO workflows with HPC               |
| 15:00 | *Coffee break*                              |
| 15:15 | Hands-on session                            |
| 16:30 | Wrap-up                                     |
| 17:00 | END                                         |
 

## 👩‍💻👩‍💼👨‍🔬🧑‍🔬🧑‍🚀🧙‍♂️🔧 Roll Call
Name/ pronouns (optional) / job, role / social media (twitter, github, ...) / background or interests (optional) / city

**data removed**


## 🧠 Collaborative Notes
 
Configure access to platform (you will need to choose an identifier):
`python runJupyterDaskOnSLURM.py --add_platform`

Install components on platform:
`python runJupyterDaskOnSLURM.py --uid IDENTIFIER --mode install`

Start a notebook on the platform:
`python runJupyterDaskOnSLURM.py --uid IDENTIFIER --mode run`

Link to the repository:
https://github.com/RS-DAT/2023-05-15-workshop-ITC

Dask delayed:
Importing delayed functionality from dask
```
from dask.delayed import delayed
```

Testing the delayed functionality
```
@delayed
def add(x,y):
    print(f"{x} + {y}")
    return x+y
```

```
a_p = add(1,2)
b_p = add(a_p, 3)
c_p = add(a_p, b_p)
```

Visualisation of workflow with dask:
```
import dask
dask.visualize(c_p, rankdir="LR")
```

To run the delayed commands:
```
c_p.compute()
```

Dask Arrays:
```
import dask array as da
```
Generate a random dask array:
```
x = da.random.random((2000, 1000), chunks=(500, 500))
x
```

Set-up a calculation:
Step 1: y = x\*x^T
```
y = da.dot(x, x.T)
y
```
Step 2: z = mean(y)
```
z = y.mean()
z
```

Visualize task graph:
```
dask.visualize(z)
```

Compute the result:
```
z.compute()
```
Xarray 
Starting from an exisiting (raster) dataset
prefilled 
```
raster_path
```

use rioxarray (xarray + rasterio to handle raster loading)

```
import rioxarray
```
open raster file
```
raster = rioxarray.open_raster(raster_path, chunks={'x':2000, 'y':2000}, lock=False)
```
additional arguments specify chunking
<b>```lock=False``` allows for parallel non-blocking reading of chunks</b>
if not set this will be serial

inspect object
```
raster
```
simple statistics over object
```
raster_max = raster.max()
```
To reiterate, no data has been loaded yet!
visualize object aggain
```
dask.visualize(raster_max)
```
compute value (this may take some time as data must now be loaded)
```
raster_max.compute()
```

**Executing task graphs**

***Multithreading***
```
raster_max.compute(scheduler='threads')
```

Note: ``` scheduler='threads'``` is the deafult argumenbt value for the ```.compute()``` method. Threads are fast to start and share memory (but limited to local node).

***Multiprocess***
```
raster_max.compute(scheduler='processes')
```
Note: processes are slower to start and don't share memory, so cummunication requires more overhead. However, they are more independet, which can be desirable.

Rule of thumb: on a local machine processes are light weight and fast. Use processes only if needed (for security or because of library requirements). Processes allow for distribution (see below) so are needed for scaling beyond a single machine

Specify number of workers (threads/processes) to use. Default value is equal to number of cores. 
```
raster_max.compute(scheduler='threads', num_workers=2)
```

All of this works on both a local/laptop dask session as well as on a remote system.

NB: ```.compute()``` vs. ```.persist()```. The former requests the results from the computation to be returned from the workers to the scheduler and client. The latter triggers the computation, but leaves the results on the worker.

***Distributed Scheduler***

Dask extension in side bar. Use GUI to set number of workers and scale up by clicking ```scale``` button

The cluster can now be added by dragging it into the notebook, resulting in a cell incl. 
```
client = Client("ipaddress here")
```
additional diagnostic windows can be accessed from the side pane. E.g. task stream (colored bars), showing which tasks were performed where, Progress, and Workers Memory, monitoring memory usaage by the workers.
Red in the task bar signifies communication (a lot of red means that tings may be poorly parallelized)

Note: Once a ```client``` has been intialzied and registerd, it become the default option for the ```scheduler``` argument in ```.compute()``` replacing ```threads```.


Shuting down a cluster (important to release resources to keep your billing down!) simply click ```SHUTDOWN```

Then, if/when you need a new cluster, simply use the GUI again.

NB: guidelines for asking for workers. As small as possible (lifetime) for basic computing task, butt as large as needed. Cluster can be scaled up while busy (if needed, may lead to workers failing due to being out of memory). Scaling down can cause errors. This system is well suited to identifyng requirements and informing follow up, non-interactive pipelines. 

Cluster becomes available immediately and can be used with whatever number of workers are available, even while waiting for the rest to join. If a minimum number is required one can specify a break point at which execution will wait using ```client.wait_for_workers(n_workers=xx)```, where ```xx``` is the number of workers. 


**Dask in practice: Calculating Spring Indices**
Extended spring indices. For more info see: https://doi.org/10.1016/j.agrformet.2018.06.028


Import the relevant packages

Input parameters (only one year for demo purpose here). Start and enddate reflect prior knowledge on growing season. The set a bounding box in latitude and longitude to select our reason of interest (we are agian limiting the extent to finish whhith the time of this workshop)
```
year=1980

startdate = 1
enddate = 300

bbox_latlon = (-124.784, 24.753, -66.951, 49.346)
```

```
root_urlpath = "dcache://pnfs/grid.sara.nl/data/remotesensing/disk"
```

NB: STAC catalog on dCache. Can be inspected online via SURF portal. As you can see some data is retrieved , the rest has links to external data and is only retrieved when needed.

```
catalog_urlpath =f"{root_urlpath}/daymet-daily-v4/catalog.json"

output_urlpath = f"~/spring_index_models.zarr"
```

***loading/defining needed functions***
Now we run the definition of the model functions, providing these for future use in the script

***load catalog***
first define catalog
```
catalog = pystac.Catalog.from_file(catalog_urlpath)
```

inspect catalog in rich represenation using

```catalog```

With the catalog in place, lets make sure we can handel and process the data. Make sue the data in the coordiante system we want to use. Define a transformer

read CRS
```
_item = next(catalog.get_all_items())
proj_json = _item.properties["proj:projjson"]
crs_lcc = pyproj.CRS.from_json_dict(proj_json)
```

set up the converter
```
transformer = pyproj.Transformer.from_crs(
    crs_from="EPSG:4326",
    crs_to=crs_lcc,
    always_xy=True,
    )
```

```
# Calculate bbox in the dataset's CRS
bbox = transformer.transform_bounds(*bbox_latlon)
```
Now start up/scale cluster (3 workers for all participant groups!)

wait for required number of workers
```client.wait_for_workers(n_workers=3)```

now we define a function to open the data set
```
def open_dataset(urlpaths, **kwargs):
    ofs = fsspec.open_files(urlpaths, block_size=4*2**20)
    return xr.open_mfdataset(
        [of.open() for of in ofs],
        engine='h5netcdf',
        decode_coords='all',
        drop_variables=("lat","lon"),
        **kwargs
    )
```

Siince we have a function to open datasets, which works on (multiple) urlpaths we can create a list with all such files and then porogrammttically open them.

First we create the list

```
item = catalog.get_item(f"na-{year}",recursive=True)
hrefs = [
    item.assets[var].get_absolute_href()
    for var in("tmin","tmax",dayl")
]
```

now we open them all together as a single dataset (and coarsen the dataset for the purposes of managable computing time in the demo)


```
ds.open_dataset(hrefs, chunks={"time":5, "x":1000, "y":1000})
ds = ds.coarsen({"x":20,"y":20}, boundary="trim").mean()
```

Note: This is all being done 'lazily' nothing has actually been read yet, only the isntructuons of what to read in what way have been provided

Now we actually plot the perdictors at time `t=0` This involves only the data for this time being read and used in the computation

ds["tmax"].isel(time=0).plot.imshow()

Dask was fast, because not all the data was read, however, if you want the same for a differnt time, thhe releavant data must still be read. SO: think about what you need/want to read when


define preprocessing funtion
```
def preprocess_dataset(ds, startdate, enddate, bbox):

ds = ds.isel(time=slice(startdate-1,enddate))
ds = ds.rio.clip_box(*bbox)

tmax = ds["tmax"]*1.8 + 32
tmin = ds["tmin"]* 1.8 + 32

dayl = ds["dayl"]/3600

return tmax,tmin,dayl
```

```tmax, tmin, dayl = preprocess_dataset(ds, startdate, enddate, bbox)```

```tmax.isel(time=0).plot.imshow()```

now we can calculate the derived property ```gdh``` (all still lazily)

```
gdh = calculate_gdh(dayl, tmin, tmax)
```

Now we will rechunk ```gdh```, as we want to consider all datapoints in time together, the chunk size will encompass the entire time range, but be divided in ```x``` and ```y```

```
gdh = gdh.chunk({"time":enddate-startdate+1,"x":200,"y":200})
```
As all the data needs to be loaded for the rechunking this takes a noticeable amount of time. However, doing so now will be beneficial for the efficiency further down the processing chain, warranting the investment here.

plot gdh curve for a given ```x,y``` position.
```
gdh.isel(x=2800, y=1500).plott()
```

now we can use the metrics we've just calculated to derive the predictors

```
dde2, dd57, mds0, synop = calculate_leaf_predictors(gdh)
first_leaf = calculate_first_leaf(dde2,dd57,mds0,synop)
```
this is using the fucntions defined at the beginning of the notebook, and is also still 'lazy'

now we trigger the calculation

```first_leaf = client.persist([first_leaf])```

now we can check our first leaf index out

``` first_leaf[0].plot.imshow(col="plant")```

Now we can save this computed data using zarr (a natively chunked format) (see provided function)

while saving we will rechunk to our needs
save_to_urlpath(
    first_leaf.chunk({"plant":1,"x":1000,"y":1000}),
    output_urlpath,
    f"{year}",
    )




## 📚 Resources

* [SURF](https://www.surf.nl)
* [Apply for access to SURF services](https://www.surf.nl/en/research-it/apply-for-access-to-compute-services) 


