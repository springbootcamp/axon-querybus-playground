@file:Suppress("unused")

package org.springbootcamp.axon.querybus

import mu.KLogging
import org.axonframework.commandhandling.CommandHandler
import org.axonframework.commandhandling.TargetAggregateIdentifier
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.commandhandling.model.AggregateIdentifier
import org.axonframework.commandhandling.model.AggregateLifecycle.apply
import org.axonframework.eventhandling.EventHandler
import org.axonframework.eventsourcing.EventSourcingHandler
import org.axonframework.queryhandling.QueryGateway
import org.axonframework.queryhandling.QueryHandler
import org.axonframework.spring.stereotype.Aggregate
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component

sealed class Command {
  data class CreateAccount(@TargetAggregateIdentifier val id: String)
  data class Deposit(@TargetAggregateIdentifier val id: String, val amount: Int)
}

sealed class Event {
  data class AccountCreated(val id: String, val balance: Int = 0)
  data class BalanceChanged(val id:String, val amount: Int)
}

sealed class Query {
  data class BalanceRequest(val id: String)
  data class BalanceResponse(val amount: Int)
}

@Aggregate
class AccountAggregate() {

  companion object : KLogging()

  @CommandHandler
  constructor(c: Command.CreateAccount) : this() {
    apply(Event.AccountCreated(id = c.id))
  }

  @CommandHandler
  fun handle(c: Command.Deposit) {
    apply(Event.BalanceChanged(c.id, c.amount))
  }

  @AggregateIdentifier
  lateinit var id: String
  var balance: Int = 0

  @EventSourcingHandler
  fun on(e: Event.AccountCreated) {
    id = e.id
    balance = e.balance
  }

  @EventSourcingHandler
fun on(e: Event.BalanceChanged) {
    balance += e.amount
  }
}

@Component
class AccountView {
  companion object : KLogging()

  private val accounts: MutableMap<String, Int> = mutableMapOf()

  @EventHandler
   fun on(e: Event.AccountCreated) {
    accounts[e.id] = e.balance
    logger.info { "added account: $e" }
  }

  @EventHandler
  fun on(e: Event.BalanceChanged) {

    accounts.computeIfPresent(e.id, {k,v -> v + e.amount})
    logger.info { "changed amount: $e" }
  }


  @QueryHandler
  private fun on(q: Query.BalanceRequest) = Query.BalanceResponse(accounts[q.id] ?: 0)
}

@SpringBootApplication
class Application(
  private val commandGateway: CommandGateway,
  private val queryGateway: QueryGateway
) : CommandLineRunner {

  companion object : KLogging()


  override fun run(vararg args: String?) {
    with(commandGateway) {
      send<Command.CreateAccount>(Command.CreateAccount("1"))
      send<Command.Deposit>(Command.Deposit("1", 100))
    }

    logger.info { "result: ${queryGateway.query(Query.BalanceRequest("1"), Query.BalanceResponse::class.java).join()}" }
  }

}

fun main(args: Array<String>) = runApplication<Application>(*args).let { Unit }

