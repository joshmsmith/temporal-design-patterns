
# QoS & Throughput Patterns

Patterns for controlling how fast work executes, protecting downstream services from overload, and ensuring that no single caller or tenant can monopolize Worker capacity at the expense of others.

<div class="pattern-grid">
<div class="pattern-tile">
<a href="downstream-rate-limiting">
<div class="pattern-tile-header">
<span>Downstream Rate Limiting</span>
</div>
<p>Caps Activity execution rate against a downstream service by routing throttled Activities to a dedicated Task Queue backed by Workers configured with a throughput limit.</p>
</a>
</div>

<div class="pattern-tile">
<a href="priority-task-queues">
<div class="pattern-tile-header">
<span>Priority Task Queues</span>
</div>
<p>Assigns a priority level to Workflows and Activities so that time-sensitive work executes ahead of lower-priority work within a single Task Queue.</p>
</a>
</div>

<div class="pattern-tile">
<a href="fairness">
<div class="pattern-tile-header">
<span>Fairness</span>
</div>
<p>Distributes Worker capacity evenly across tenants or users so that a burst from one caller does not starve the others.</p>
</a>
</div>
</div>
