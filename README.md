Pregel+: A Distributed Graph Computing Framework with Effective Message Reduction
===============

Pregel+ is not just another open-source Pregel implementation, but a substantially improved distributed graph computing system with effective message reduction. Compared with existing Pregel-like systems, Pregel+ provides simpler programming interface and yet achieves higher computational efficiency. We give ample examples and detailed tutorials to demonstrate how to program in Pregel+ and deploy Pregel+ in a distributed environment. Pregel+ is also a better choice for researchers who want to change the system to support new functionalities, as the design of Pregel+ is much simpler and more flexible than most other Pregel-like systems.

Pregel+ supports two effective message reduction techniques: (1)vertex mirroring and (2)a new request-respond paradigm. These techniques not only reduce the total number of messages exchanged through the network, but also bound the number of messages sent/received by any vertex, especially for processing power-law graphs and (relatively) dense graphs.

Performance Highligts
===============

<ul><li><p>Running Hash-Min, an algorithm for computing connected components of a small-diameter graph, over the BTC dataset in a cluster with 1 master machine and 15 slave machines each running 8 workers (here, "workers" refer to "processes" for Pregel+ and GPS, and "threads" for GraphLab and Giraph).</p></li></ul>

<table border="1">
  <tr>
    <td width="100" align="center"><b>Pregel+</b></td>
    <td width="190" align="center"><b>Pregel+ with Mirroring</b></td>
    <td width="190" align="center"><b>Giraph</b></td>
    <td width="190" align="center"><b>GraphLab Sync</b></td>
    <td width="190" align="center"><b>GraphLab ASync</b></td>
    <td width="190" align="center"><b>GPS</b></td>
    <td width="190" align="center"><b>GPS LALP</b></td>
  </tr>
  <tr>
    <td width="100" align="center"><b>27 seconds</b></td>
    <td width="190" align="center"><b><font color="red"><i>10 seconds*</i></font></b></td>
    <td width="190" align="center"><b>93 seconds</b></td>
    <td width="190" align="center"><b>83 seconds</b></td>
    <td width="190" align="center"><b>155 seconds</b></td>
    <td width="190" align="center"><b>38 seconds</b></td>
    <td width="190" align="center"><b>33 seconds</b></td>
  </tr>
</table>

<ul><li><p>Number of messages sent by each worker using Pregel+ (blue bars — ordinary mode, red bars — mirroring)</p></li></ul>
![](http://www.cse.cuhk.edu.hk/pregelplus/figs/msgNum_btc.jpg)

<ul><li><p>Running S-V, an algorithm for computing connected components of a large-diameter graph, over the USA road network in a cluster with 1 master machine and 15 slave machines each running 4 workes. GraphLab is inapplicable since it does not support vertex communication with a non-neighbor, which is required by S-V.</p></li></ul>

<table border="1">
  <tr>
    <td width="100" align="center"><b>Pregel+</b></td>
    <td width="190" align="center"><b>Pregel+ with Request-Respond Paradigm</b></td>
    <td width="190" align="center"><b>Giraph</b></td>
    <td width="190" align="center"><b>GraphLab</b></td>
    <td width="190" align="center"><b>GPS</b></td>
  </tr>
  <tr>
    <td width="100" align="center"><b>262 seconds</b></td>
    <td width="190" align="center"><b><font color="red"><i>138 seconds*</i></font></b></td>
    <td width="190" align="center"><b>690 seconds</b></td>
    <td width="190" align="center"><b>inapplicable</b></td>
    <td width="190" align="center"><b>190 seconds</b></td>
  </tr>
</table>

<ul><li><p>Number of messages sent by each worker using Pregel+ (blue bars — ordinary mode, red bars — request-respond paradigm)</p></li></ul>
![](http://www.cse.cuhk.edu.hk/pregelplus/figs/msgNum_usa.jpg)
