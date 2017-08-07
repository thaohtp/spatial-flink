package config

import org.springframework.context.annotation._
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** Experiments definitions for the 'spatial-analysis' bundle. */
@Configuration
@ComponentScan( // Scan for annotated Peel components in the 'de.tu_berlin.dima.bdapro' package
  value = Array("de.tu_berlin.dima"),
  useDefaultFilters = false,
  includeFilters = Array[ComponentScan.Filter](
    new ComponentScan.Filter(value = Array(classOf[org.springframework.stereotype.Service])),
    new ComponentScan.Filter(value = Array(classOf[org.springframework.stereotype.Component]))
  )
)
@ImportResource(value = Array(
  "classpath:peel-core.xml",
  "classpath:peel-extensions.xml"
))
@Import(value = Array(
  classOf[org.peelframework.extensions],   // custom system beans
  classOf[config.fixtures.systems],        // custom system beans
  classOf[config.fixtures.wordcount],      // wordcount experiment beans
  classOf[config.fixtures.oddsemordnilaps],
  classOf[config.fixtures.palindrome],
  classOf[config.fixtures.spatialanalysiss1],
  classOf[config.fixtures.spatialanalysiss5],
  classOf[config.fixtures.spatialanalysiss10],
  classOf[config.fixtures.spatialanalysiss1_v0],
  classOf[config.fixtures.spatialanalysiss1_v1],
  classOf[config.fixtures.spatialanalysiss5_v0],
  classOf[config.fixtures.spatialanalysiss5_v1],
  classOf[config.fixtures.spatialanalysiss10_v0],
  classOf[config.fixtures.spatialanalysiss10_v1],
  classOf[config.fixtures.simba1],
  classOf[config.fixtures.simba5],
  classOf[config.fixtures.simba10],
  classOf[config.fixtures.circlerangequery],
  classOf[config.fixtures.knnquery],
  classOf[config.fixtures.distancejoin],
  classOf[config.fixtures.knnjoin]
))
class experiments extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }
}
