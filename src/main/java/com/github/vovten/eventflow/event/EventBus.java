package com.github.vovten.eventflow.event;

/**
 * <h2>Event bus type for managing message distribution</h2>
 *
 * <p>Defines the boundaries of event propagation in the system:</p>
 * <ul>
 *    <li><b>INTERNAL</b> - high-performance in-memory bus inside the JVM</li>
 *    <li><b>EXTERNAL</b> - distributed fault-tolerant bus for the cluster</li>
 * </ul>
 *
 * <h3>Selection criteria:</h3>
 * <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
 *    <tr>
 *        <th width="20%"><b>Bus type</b></th>
 *        <th width="40%"><b>Usage</b></th>
 *        <th width="40%"><b>Implementation</b></th>
 *    </tr>
 *    <tr>
 *        <td><code>INTERNAL</code></td>
 *        <td>
 *            <ul>
 *                <li>Synchronous notifications within the process</li>
 *                <li>Data transfer between modules</li>
 *                <li>Local resource management</li>
 *            </ul>
 *        </td>
 *        <td>
 *            <ul>
 *                <li><code>BlockingDeque</code> (primary implementation)</li>
 *            </ul>
 *        </td>
 *    </tr>
 *    <tr>
 *        <td><code>EXTERNAL</code></td>
 *        <td>
 *            <ul>
 *                <li>Coordination between microservices</li>
 *                <li>Event publishing to all service replicas</li>
 *                <li>System-wide broadcast notifications</li>
 *            </ul>
 *        </td>
 *        <td>
 *            <ul>
 *                <li><b>Apache Kafka</b> (primary implementation)</li>
 *            </ul>
 *        </td>
 *    </tr>
 *    <tr>
 *        <td colspan="3">
 *            <table border="1" cellpadding="5">
 *                <tr>
 *                    <th>Metric</th>
 *                    <th>INTERNAL</th>
 *                    <th>EXTERNAL</th>
 *                </tr>
 *                <tr>
 *                    <td>Throughput</td>
 *                    <td>1M msg/sec</td>
 *                    <td>Depends on Kafka configuration</td>
 *                </tr>
 *            </table>
 *        </td>
 *    </tr>
 * </table>
 */
public enum EventBus {
    INTERNAL,
    EXTERNAL
}
