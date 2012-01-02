<map version="0.9.0">
<!-- To view this file, download free mind mapping software FreeMind from http://freemind.sourceforge.net -->
<node CREATED="1306311122088" ID="ID_1808886292" MODIFIED="1307025756614" TEXT="JMS implementation">
<node CREATED="1307023901598" HGAP="12" ID="ID_261001130" MODIFIED="1307027221409" POSITION="left" TEXT="Task 2: implementation" VSHIFT="-82">
<node CREATED="1307026733604" ID="ID_1055127432" MODIFIED="1307026751344" TEXT="design">
<node CREATED="1307023911757" HGAP="36" ID="ID_292671421" MODIFIED="1307026739640" TEXT="separate from Hedwig core" VSHIFT="-29">
<node CREATED="1307024645311" ID="ID_1106138135" MODIFIED="1307024662383" TEXT="no or minimal modifications in hedwig core">
<node CREATED="1323858433315" ID="ID_734502123" MODIFIED="1323858475950" TEXT="filtering implemented on the client side">
<icon BUILTIN="full-2"/>
</node>
<node CREATED="1323858481794" ID="ID_410221762" MODIFIED="1323858488985" TEXT="filtering on the server side">
<icon BUILTIN="full-3"/>
</node>
</node>
</node>
<node CREATED="1307023925117" ID="ID_358249961" MODIFIED="1307023949441" TEXT="wrappers around Hedwig&apos;s methods"/>
</node>
<node CREATED="1307026754099" HGAP="16" ID="ID_388444426" MODIFIED="1307035540761" TEXT="iterative development" VSHIFT="22">
<node CREATED="1307026762537" ID="ID_515775462" MODIFIED="1307441914646" TEXT="from simplest to more complex"/>
<node CREATED="1307026811368" ID="ID_957403597" MODIFIED="1310976947363" TEXT="include regression tests"/>
</node>
</node>
<node CREATED="1307023956892" HGAP="-17" ID="ID_820597274" MODIFIED="1307027241961" POSITION="left" TEXT="Task 3: validation" VSHIFT="83">
<node CREATED="1307023961360" ID="ID_1780831635" MODIFIED="1310978933106" TEXT="conformance tests">
<icon BUILTIN="full-4"/>
</node>
<node CREATED="1307023973267" ID="ID_1178710450" MODIFIED="1310978935666" TEXT="performance tests">
<icon BUILTIN="full-4"/>
<node CREATED="1307023979288" ID="ID_1733659972" MODIFIED="1307035566561" TEXT="no &quot;free&quot; standard-&gt; need to define our own benchmarks"/>
</node>
</node>
<node CREATED="1307025757696" ID="ID_1855013489" MODIFIED="1307027224330" POSITION="right" TEXT="Task 1: evaluate feasibility / things to do">
<node CREATED="1306311202887" HGAP="30" ID="ID_1368507752" MODIFIED="1307025823605" TEXT="Concepts mapping" VSHIFT="-112">
<node CREATED="1306311460693" ID="ID_485728338" MODIFIED="1306311524591" TEXT="administration">
<node CREATED="1306311524592" ID="ID_598597411" MODIFIED="1310978504152" TEXT="JNDI mapping">
<icon BUILTIN="messagebox_warning"/>
<icon BUILTIN="full-1"/>
</node>
<node CREATED="1306311530683" ID="ID_586484960" MODIFIED="1324467513332" TEXT="session pool">
<icon BUILTIN="messagebox_warning"/>
<icon BUILTIN="full-2"/>
<icon BUILTIN="button_cancel"/>
<node CREATED="1324467516782" ID="ID_1751717161" MODIFIED="1324467521334" TEXT="optional"/>
</node>
<node CREATED="1306313171196" ID="ID_1544584557" MODIFIED="1325519038918" TEXT="administered objects">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
<node CREATED="1306313176025" ID="ID_1995030660" MODIFIED="1306313181555" TEXT="destination"/>
<node CREATED="1306313182724" ID="ID_959788732" MODIFIED="1306313188836" TEXT="connection factory"/>
</node>
</node>
<node CREATED="1306311634128" ID="ID_1549616726" MODIFIED="1306311746077" TEXT="messages">
<node CREATED="1306311751349" ID="ID_653411647" MODIFIED="1324467502252" TEXT="header fields">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
</node>
<node CREATED="1306311779412" ID="ID_1626765482" MODIFIED="1324467462825" TEXT="properties">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
</node>
<node CREATED="1306311814524" ID="ID_1845501448" MODIFIED="1323858388173" TEXT="selectors">
<icon BUILTIN="full-2"/>
<icon BUILTIN="button_ok"/>
<node CREATED="1307023797514" ID="ID_934854605" MODIFIED="1325516932167" TEXT="SQL-like, needs parsing">
<icon BUILTIN="button_ok"/>
</node>
</node>
<node CREATED="1306311918864" ID="ID_121658079" MODIFIED="1323858393016" TEXT="body types">
<icon BUILTIN="full-2"/>
<icon BUILTIN="button_ok"/>
<node CREATED="1307023875164" ID="ID_1560781808" MODIFIED="1325516941109" TEXT="standard body types to provide">
<icon BUILTIN="button_ok"/>
<node CREATED="1310817311851" ID="ID_975408468" MODIFIED="1310817327113" TEXT="serialization / deserialization facilities required"/>
</node>
</node>
<node CREATED="1325516785996" ID="ID_642036658" MODIFIED="1325516807016" TEXT="priorities">
<icon BUILTIN="full-2"/>
<icon BUILTIN="messagebox_warning"/>
</node>
</node>
<node CREATED="1306313164292" HGAP="42" ID="ID_1373536556" MODIFIED="1310810697256" TEXT="connection" VSHIFT="11">
<node CREATED="1306313284709" ID="ID_753760533" MODIFIED="1310978550534" TEXT="authentication">
<icon BUILTIN="messagebox_warning"/>
<icon BUILTIN="full-2"/>
<node CREATED="1310746800709" ID="ID_234132272" MODIFIED="1310746821754" TEXT="provide basic user/pass scheme"/>
<node CREATED="1310746840355" ID="ID_1249563563" MODIFIED="1310746862166" TEXT="more sophisticated auth scheme are not specified by the JMS spec"/>
<node CREATED="1310746886034" ID="ID_1916667560" MODIFIED="1310746909072" TEXT="auth schemes should be pluggable, both on client and server side"/>
</node>
<node CREATED="1306313288537" ID="ID_859721412" MODIFIED="1324467458893" TEXT="clientID">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
</node>
<node CREATED="1306313292384" ID="ID_1593215052" MODIFIED="1323858631332" TEXT="pausing">
<icon BUILTIN="full-2"/>
<icon BUILTIN="button_ok"/>
</node>
<node CREATED="1306313314168" ID="ID_272035651" MODIFIED="1324467456373" TEXT="closing">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
</node>
</node>
<node CREATED="1306313203179" HGAP="43" ID="ID_1990830921" MODIFIED="1310810699094" TEXT="session" VSHIFT="21">
<node CREATED="1306313238146" ID="ID_243421499" MODIFIED="1324467449249" TEXT="transactions">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
<node CREATED="1306313603356" ID="ID_741944448" MODIFIED="1306313614913" TEXT="transacted session (commit/rollback)"/>
<node CREATED="1306313616071" ID="ID_352993296" MODIFIED="1307441810250" TEXT="distributed transactions (XA)">
<icon BUILTIN="button_cancel"/>
<node CREATED="1325516900082" ID="ID_1532441015" MODIFIED="1325516902843" TEXT="optional"/>
</node>
</node>
<node CREATED="1306313242530" ID="ID_20747239" MODIFIED="1324467448183" TEXT="ordering">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
<node CREATED="1325516817348" ID="ID_332094875" MODIFIED="1325516851237" TEXT="this currently relies on synchronous publishing due to BOOKKEEPER-34">
<icon BUILTIN="info"/>
</node>
</node>
<node CREATED="1306313253802" ID="ID_1678417152" MODIFIED="1324467447058" TEXT="acknowledgement">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
</node>
<node CREATED="1306313326871" ID="ID_1749907494" MODIFIED="1324467444208" TEXT="temporary destinations">
<icon BUILTIN="full-2"/>
<icon BUILTIN="button_ok"/>
<node CREATED="1312468797639" ID="ID_1542428079" MODIFIED="1312468799762" TEXT="topic"/>
<node CREATED="1312468800499" ID="ID_1495313581" MODIFIED="1312468801769" TEXT="queue"/>
</node>
<node CREATED="1306313339862" ID="ID_82080080" MODIFIED="1324467526083" TEXT="closing">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
</node>
<node CREATED="1307442017146" ID="ID_279195365" MODIFIED="1323861266419" TEXT="pooling">
<icon BUILTIN="full-2"/>
<icon BUILTIN="button_cancel"/>
<node CREATED="1323861268373" ID="ID_762424632" MODIFIED="1323861271812" TEXT="optional"/>
</node>
<node CREATED="1307442378979" ID="ID_1455272012" MODIFIED="1310978575941" TEXT="concurrency">
<icon BUILTIN="full-2"/>
</node>
</node>
<node CREATED="1306313363358" HGAP="43" ID="ID_1712477491" MODIFIED="1310810707917" TEXT="consumers" VSHIFT="10">
<node CREATED="1306313375478" ID="ID_1159048102" MODIFIED="1324467436156" TEXT="synchronous">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
<node CREATED="1312468910481" ID="ID_900303675" MODIFIED="1312468915536" TEXT="receive() methods"/>
</node>
<node CREATED="1306313378998" ID="ID_813373112" MODIFIED="1324467435343" TEXT="asynchronous">
<icon BUILTIN="full-1"/>
<icon BUILTIN="button_ok"/>
<node CREATED="1312468902218" ID="ID_285477977" MODIFIED="1312468907324" TEXT="MessageListener"/>
</node>
</node>
<node CREATED="1306313370382" HGAP="34" ID="ID_621263040" MODIFIED="1310810706054" TEXT="producers" VSHIFT="21"/>
<node CREATED="1306313408293" ID="ID_1390379890" MODIFIED="1310550200633" TEXT="communication models">
<node CREATED="1306313416044" ID="ID_1593675369" MODIFIED="1310978588316" TEXT="point to point">
<icon BUILTIN="full-3"/>
<node CREATED="1306313425221" ID="ID_613412778" MODIFIED="1310550230587" TEXT="queues">
<icon BUILTIN="messagebox_warning"/>
</node>
</node>
<node CREATED="1306313435764" ID="ID_607066504" MODIFIED="1310978535054" TEXT="pub/sub">
<icon BUILTIN="full-1"/>
<node CREATED="1306313449020" ID="ID_1750316175" MODIFIED="1324467537051" TEXT="topics">
<icon BUILTIN="button_ok"/>
</node>
<node CREATED="1306313451332" ID="ID_162594167" MODIFIED="1324467539923" TEXT="durable subscriptions">
<icon BUILTIN="button_ok"/>
</node>
<node CREATED="1310550213632" ID="ID_253544012" MODIFIED="1325516883705" TEXT="non durable subscriptions">
<icon BUILTIN="button_ok"/>
</node>
</node>
<node CREATED="1310550121068" ID="ID_468153108" MODIFIED="1310978582004" TEXT="persistent messages">
<icon BUILTIN="full-1"/>
</node>
<node CREATED="1310550200634" ID="ID_1338176962" MODIFIED="1310978584582" TEXT="non persistent messages">
<icon BUILTIN="messagebox_warning"/>
<icon BUILTIN="full-2"/>
</node>
</node>
<node CREATED="1306313482043" ID="ID_834129440" MODIFIED="1306313485578" TEXT="exceptions"/>
</node>
<node CREATED="1306314457406" HGAP="101" ID="ID_225916724" MODIFIED="1307025869697" TEXT="API" VSHIFT="-129">
<node CREATED="1306314462710" ID="ID_1700597903" MODIFIED="1306315413971" TEXT="43 interfaces">
<node CREATED="1306315367373" ID="ID_587149795" MODIFIED="1307441825678" TEXT="9 for XA (distributed transactions)">
<icon BUILTIN="button_cancel"/>
</node>
</node>
<node CREATED="1306315420555" ID="ID_1399377383" MODIFIED="1306315422978" TEXT="2 classes"/>
<node CREATED="1306315438731" ID="ID_824077995" MODIFIED="1306315442754" TEXT="13 exceptions"/>
</node>
</node>
</node>
</map>
