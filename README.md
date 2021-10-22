# mutualfund
Offline Trackling of Mutual Fund Portfolio

Current Platforms like kuvera, paytm, icicidirect, coin etc all are very good investment option but they all maintain our information on server and this personal and financial information can be sold to others. The others can utilize this information in many number of ways targeted advertising, finantial fraud, HNI targetting for mallatious means. We have control over this as our information is stored in company servers. 


The repository consist of a offline codebase which will store all the personal data in local storage and will only connect to internet to retrieve current NAV values of Mutual funds. 

## Highlights

* <b>Sqlite [Local]</b> - Storing the finantial information
* <b>Nav Update</b> - Based on command line or scheduled cron, to retrieve latest NAV from internet - http://portal.amfiindia.com
* <b>CAS Statement</b> - Downloaded from Kfintech
* <b>Python Server [Local]</b> - Brrowsing the Portfolio, user can host it locally or hosted runtime under VPN.

