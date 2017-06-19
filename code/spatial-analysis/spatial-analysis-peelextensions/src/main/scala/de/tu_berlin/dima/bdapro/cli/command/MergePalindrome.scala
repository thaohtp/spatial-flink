package de.tu_berlin.dima.bdapro.cli.command

import org.springframework.stereotype.Service

/** Verify and merge the warmup tasks into the current branch. */
@Service("merge:palindrome")
class MergePalindrome extends MergeTask {

  def commitMsg(user: String): String =
    s"[WARMUP] $taskName solution from '$user'."

  override val taskName = "Palindrome"

  override val taskBranch = "palindrome"
}
