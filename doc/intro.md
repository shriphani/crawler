# Introduction to crawler

Introduction:

The crawler cannot entirely be built using lein deps. I pushed a few
commits to wtetzner's uri library that we use so we have to run
<code>lein install</code> on it to make things work:

<pre>
 git clone https://github.com/wtetzner/exploding-fish.git
 cd exploding-fish
 lein install
</pre>

Next, install natty:

<pre>
git clone https://github.com/joestelmach/natty.git
cd natty
export MAVEN_OPTS=-Xmx1024m
mvn install
</pre>

After this, this project can be run from the repl (currently the only
way to use it).
