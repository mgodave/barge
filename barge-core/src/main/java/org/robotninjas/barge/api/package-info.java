/**
 * <h2>Domain Model Objects</h2>
 * <p>This package group the <em>domain model objects</em> for Raft protocol implementation. These are POJOs which
 * are used to represent:</p>
 * <ul>
 *   <li>The messages exchanged between raft instances part of the same replica set
 *   through {@link org.robotninjas.barge.rpc.RaftClient},</li>
 *   <li>The entries in the {@link org.robotninjas.barge.log.RaftLog} that persists the state of each replica.</li>
 * </ul>
 */
package org.robotninjas.barge.api;

